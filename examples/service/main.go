// Command service demonstrates HTTP queries backed by gRPC, with injected tracing and Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	"github.com/Waterkyuu/soroi/examples/service/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// user is the immutable public profile returned by HTTP and retained by soroi.
type user struct {
	// Name is the display name obtained from the gRPC backend.
	Name string `json:"name"`
}

// main selects a finite demonstration or a localhost HTTP server with graceful cancellation.
func main() {
	listen := flag.String("listen", "", "HTTP address; empty runs two demonstration requests and exits")
	traces := flag.Bool("traces", false, "write OpenTelemetry spans as JSON to stderr")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	traceOutput := io.Discard
	if *traces {
		traceOutput = os.Stderr
	}
	err := run(ctx, *listen, traceOutput)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run owns the example's gRPC server, connection, query client, registry and tracer provider.
func run(ctx context.Context, address string, traceOutput io.Writer) error {
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(traceOutput))
	if err != nil {
		return err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(provider), otelgrpc.WithPropagators(propagation.TraceContext{}))))
	var calls atomic.Int32
	rpc.Register(server, func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		calls.Add(1)
		return wrapperspb.String("Alice"), nil
	})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	connection, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(provider),
			otelgrpc.WithPropagators(propagation.TraceContext{}))),
	)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	budget, err := soroi.NewExecutor(soroi.ExecutionOptions{MaxConcurrent: 16, MaxQueued: 0})
	if err != nil {
		return err
	}
	client := soroi.NewClient(soroi.Options{Executor: budget, StaleTime: time.Minute, Timeout: time.Second})
	defer client.Close()
	handler, err := newHandler(client, connection, provider, prometheus.NewRegistry())
	if err != nil {
		return err
	}
	if address != "" {
		return serve(ctx, address, handler)
	}
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	for range 2 {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/users/42", nil)
		if err != nil {
			return err
		}
		response, err := httpServer.Client().Do(request)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			return readErr
		}
		fmt.Printf("HTTP %d: %s", response.StatusCode, body)
	}
	fmt.Printf("gRPC source calls: %d\nlocal cache hits: %d\n", calls.Load(), client.Stats().CacheHits)
	return nil
}

// newHandler shares one query client across requests and keeps raw identifiers out of telemetry labels.
func newHandler(
	client *soroi.Client,
	connection grpc.ClientConnInterface,
	provider trace.TracerProvider,
	registry *prometheus.Registry,
) (http.Handler, error) {
	duration, err := registerMetrics(client, registry)
	if err != nil {
		return nil, err
	}
	tracer := provider.Tracer("soroi-example")
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.Handle("GET /users/{id}", otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "user.fetch")
		defer span.End()
		started := time.Now()
		id := r.PathValue("id")
		// Profiles in this example are public; private data needs authenticated scope in this comparable key.
		value, err := soroi.Fetch(ctx, client, id, func(ctx context.Context) (user, error) {
			name, err := rpc.Get(ctx, connection, id)
			return user{Name: name}, err
		})
		outcome := "success"
		if err != nil {
			outcome = "error"
			code := errorStatus(err)
			switch code {
			case http.StatusServiceUnavailable:
				outcome = "overload"
			case http.StatusGatewayTimeout:
				outcome = "timeout"
			case http.StatusRequestTimeout:
				outcome = "canceled"
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, "query failed")
			duration.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
			http.Error(w, http.StatusText(code), code)
			return
		}
		duration.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	}), "GET /users/{id}", otelhttp.WithTracerProvider(provider), otelhttp.WithPropagators(propagation.TraceContext{})))
	return mux, nil
}

// serve releases the HTTP listener and active requests when the application context is canceled.
func serve(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			_ = server.Close()
			return err
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
