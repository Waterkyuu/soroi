package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
	"github.com/Waterkyuu/soroi/examples/service/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestServiceIntegration verifies HTTP-to-gRPC loading, cache reuse, metric scraping and parent propagation.
func TestServiceIntegration(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(provider), otelgrpc.WithPropagators(propagation.TraceContext{}))))
	var calls atomic.Int32
	rpc.Register(server, func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		calls.Add(1)
		return wrapperspb.String("Alice"), nil
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///users",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(provider),
			otelgrpc.WithPropagators(propagation.TraceContext{}))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	t.Cleanup(client.Close)
	handler, err := newHandler(client, connection, provider, prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/users/42", nil)
		request.Header.Set("Traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "{\"name\":\"Alice\"}\n" {
			t.Fatalf("HTTP = %d %s", response.Code, response.Body)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("gRPC source calls = %d", calls.Load())
	}
	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, metric := range []string{"soroi_cache_hits_total 1", "soroi_loads_total 1", "soroi_fetch_duration_seconds_count{outcome=\"success\"} 2"} {
		if !strings.Contains(metrics.Body.String(), metric) {
			t.Fatalf("missing %q in %s", metric, metrics.Body)
		}
	}
	var rpcSpans int
	for _, span := range exporter.GetSpans() {
		if span.SpanContext.TraceID().String() != "11111111111111111111111111111111" {
			t.Fatalf("span lost incoming trace: %s", span.Name)
		}
		if strings.Contains(span.Name, "soroi.example.Users/Get") {
			rpcSpans++
		}
	}
	if rpcSpans != 2 {
		t.Fatalf("want one gRPC client/server pair, got %d spans", rpcSpans)
	}
}

// TestHTTPErrorStatus keeps transport mapping separate from the query's error policy.
func TestHTTPErrorStatus(t *testing.T) {
	for _, test := range []struct {
		// err is the query outcome exposed by the service.
		err error
		// want is the corresponding HTTP status code.
		want int
	}{
		{err: soroi.ErrQueueFull, want: http.StatusServiceUnavailable},
		{err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
		{err: context.Canceled, want: http.StatusRequestTimeout},
	} {
		if got := errorStatus(test.err); got != test.want {
			t.Errorf("status(%v) = %d, want %d", test.err, got, test.want)
		}
	}
}
