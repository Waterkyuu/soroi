# HTTP and gRPC queries with OpenTelemetry and Prometheus

[简体中文](service-integrations.zh-CN.md)

An HTTP endpoint reads a public user profile from a gRPC backend. One application-owned
soroi Client shares results across requests. OpenTelemetry follows the request into
the backend, and Prometheus exposes cache decisions and caller-visible latency.
All framework dependencies remain in an independent example module.

## Installation

The example requires Go 1.26+ and a checkout of this repository. The core library
continues to support Go 1.22 with no dependencies. No external server, credentials,
protoc installation, or telemetry collector is required for the demonstration.

From the repository root:

```sh
cd examples/service
go mod download
go run .
```

Expected output:

```text
HTTP 200: {"name":"Alice"}
HTTP 200: {"name":"Alice"}
gRPC source calls: 1
local cache hits: 1
```

## Usage

### Share one Client across HTTP requests

`newHandler` in `main.go` borrows the Client, gRPC connection, tracer provider and
Prometheus registry created by `run`. Construction registers routes and metrics
without loading user data. The HTTP handler calls `Fetch` with `r.Context()` and
the user ID. Its callback invokes `rpc.Get` only when data must be loaded.
The type argument is inferred from the callback's `user` return type.

The example serves public data. For private data, authenticate and authorize before
cache lookup and include every result-affecting tenant or permission scope in a
comparable key. Reuse compatible loaders and policies for the same key.

### Call a gRPC backend

`rpc/users.go` supplies a minimal unary service with standard protobuf `StringValue`
messages: the request contains the ID and the response contains the display name.
It uses an explicit service descriptor to keep this demo independent of protoc.
Applications should substitute their existing generated client call for `rpc.Get`.
The loader's context carries cancellation, the overall query deadline and the trace.

### Inject tracing without global providers

`run` constructs an SDK tracer provider and passes it to `otelhttp` and `otelgrpc`.
W3C `TraceContext` propagation connects the HTTP server, `user.fetch`, gRPC client
and gRPC server spans. Enable the JSON stdout exporter on stderr with:

```sh
go run . -traces
```

Each caller gets a `user.fetch` span. A cache hit makes no gRPC call. Shared source
work belongs to the initiating request's context; joining callers do not become
additional parents of that source span. The example does not create span links
for joins. Replace the synchronous console exporter with an application-owned,
batched production exporter when integrating into a service.

### Expose Prometheus metrics

`telemetry.go` uses `CounterFunc` and `GaugeFunc` to read cumulative `Client.Stats()`
directly at scrape time. It does not reconstruct counters from diagnostic events,
whose bounded subscriptions can drop events. The `HistogramVec` records each Fetch
call, including cache hits and callers waiting for another load, with bounded
`outcome` labels: `success`, `overload`, `timeout`, `canceled`, and `error`.

Start a persistent localhost endpoint:

```sh
go run . -listen 127.0.0.1:8080 -traces
```

In another terminal:

```sh
curl --fail http://127.0.0.1:8080/users/42
curl --fail http://127.0.0.1:8080/users/42
curl --fail http://127.0.0.1:8080/metrics
```

After two reads in the freshness window, the scrape contains:

```text
soroi_cache_hits_total 1
soroi_loads_total 1
soroi_fetch_duration_seconds_count{outcome="success"} 2
```

Raw IDs, query keys and error messages are not metric labels. Each registry belongs
to one Client for its lifetime; replacing that Client requires a new registry or
an explicit application-level counter reset policy. This sample intentionally
exports a small set of query metrics, not every soroi, process or runtime statistic.

## Complete example

The program below is `examples/service/main.go`. Run it in the existing example
module from the installation step; `telemetry.go` defines `registerMetrics` and
`errorStatus`, and `rpc/users.go` defines the protobuf service. Both are included
in the checkout. The command `go run .` compiles all package files together.

```go
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
```

## Configuration

| Setting | Example | Contract |
| --- | --- | --- |
| `Options.StaleTime` | One minute | Fresh reads reuse local data without restarting freshness |
| `Options.Timeout` | One second | Bounds the whole load, including Executor admission and RPC work |
| `ExecutionOptions.MaxConcurrent` | 16 | Maximum active callbacks in this process |
| `ExecutionOptions.MaxQueued` | 0 | Reject overload instead of accumulating a queue |
| `-listen` | Empty | Run two HTTP reads and exit; set an address for persistent serving |
| `-traces` | false | Opt into console span export on stderr |

The capacity values are demonstration settings, not production sizing advice.

## Error handling and common problems

| Outcome | HTTP mapping | Caller action |
| --- | --- | --- |
| `ErrQueueFull` | 503 | Apply application overload policy; soroi does not retry admission failure |
| Context or RPC deadline | 504 | Check backend latency and the whole-load deadline |
| Context or RPC cancellation | 408, if a response can still be sent | Stop request-owned work |
| gRPC NotFound | 404 | Handle the absent resource |
| Other RPC failure | 502 | Inspect the recorded trace error; internal errors are not included in HTTP bodies |

The source preserves its original error. HTTP mapping does not add retries or
convert failures into successful cached profiles. The example sets no automatic
query retries, and does not install an application gRPC retry policy.

## Behavior and limitations

The demo owns and stops its loopback servers and closes the Client, borrowed gRPC
connection and tracer provider. In persistent mode Ctrl+C initiates HTTP shutdown.
It uses plaintext loopback gRPC and a fixed public profile source; real applications
supply transport security, authentication, authorization and real business loaders.

Diagnostic events remain useful for per-key investigation. These metrics report
query activity and caller latency; `Loads` is not an exact physical RPC count under
retries or batching. Count source calls at the backend boundary when that distinction
matters.

## Complete examples and related guides

- [Integration tests](../examples/service/service_test.go): HTTP-to-gRPC reuse, propagated trace identity, scrape output and error mapping.
- [Execution budgets](execution.en-US.md): admission, cancellation and overload contracts.
- [Shared result cache](shared-cache.en-US.md#protect-the-source-during-cache-failures): Redis failure protection with the same budget API.
- [Business benchmarks](business-benchmarks.en-US.md): latency populations and physical source work.
