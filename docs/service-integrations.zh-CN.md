# 接入 HTTP、gRPC、OpenTelemetry 与 Prometheus

[English](service-integrations.en-US.md)

HTTP 接口从 gRPC 后端读取公开用户资料，由应用持有的一个 soroi Client 跨请求共享结果。
OpenTelemetry 追踪请求到后端的调用，Prometheus 暴露缓存决策和调用方延迟。
框架依赖全部保留在独立示例模块中。

## 安装

示例要求 Go 1.26+ 和当前仓库源码。核心库仍支持 Go 1.22 且没有依赖。
演示不需要外部服务、凭据、protoc 或遥测收集器。

在仓库根目录执行：

```sh
cd examples/service
go mod download
go run .
```

预期输出：

```text
HTTP 200: {"name":"Alice"}
HTTP 200: {"name":"Alice"}
gRPC source calls: 1
local cache hits: 1
```

## 使用方式

### 在 HTTP 请求之间共享一个 Client

`main.go` 的 `newHandler` 借用 `run` 创建的 Client、gRPC 连接、tracer provider 和
Prometheus registry。构造时注册路由和指标，不加载用户数据。HTTP handler 使用
`r.Context()` 和用户 ID 调用 `Fetch`。只有需要加载数据时，回调才调用 `rpc.Get`。
类型参数由回调返回的 `user` 类型推断。

示例数据公开。私有数据必须在缓存读取前完成认证和授权，并把所有影响结果的租户或权限范围
纳入可比较的 key。同一个 key 应使用兼容的加载函数和策略。

### 调用 gRPC 后端

`rpc/users.go` 使用标准 protobuf `StringValue` 消息提供最小一元服务：请求包含 ID，
响应包含显示名称。显式服务描述避免演示依赖 protoc。应用应以现有生成客户端的方法替换
`rpc.Get`。加载函数的 context 传递取消、整次查询截止时间和 trace。

### 注入 tracing，避免全局 provider

`run` 构造 SDK tracer provider，并显式传给 `otelhttp` 和 `otelgrpc`。
W3C `TraceContext` 将 HTTP server、`user.fetch`、gRPC client 和 gRPC server span
关联起来。通过以下命令将 JSON span 导出到 stderr：

```sh
go run . -traces
```

每个调用方都有自己的 `user.fetch` span。缓存命中不会调用 gRPC。共享回源属于发起请求的
context，加入等待的调用方不会成为该回源 span 的额外父节点。示例不为加入行为创建 span link。
接入真实服务时，将同步控制台 exporter 替换为应用持有的生产批量 exporter。

### 暴露 Prometheus 指标

`telemetry.go` 使用 `CounterFunc` 和 `GaugeFunc` 在抓取时直接读取累计 `Client.Stats()`，
不从可能丢弃事件的有界诊断订阅重建计数。`HistogramVec` 记录每次 Fetch，包括缓存命中和
等待共享加载的调用方，并使用有限的 `outcome` 标签：`success`、`overload`、`timeout`、
`canceled` 和 `error`。

启动持续运行的本地接口：

```sh
go run . -listen 127.0.0.1:8080 -traces
```

在另一个终端执行：

```sh
curl --fail http://127.0.0.1:8080/users/42
curl --fail http://127.0.0.1:8080/users/42
curl --fail http://127.0.0.1:8080/metrics
```

在新鲜期内读取两次后，指标包含：

```text
soroi_cache_hits_total 1
soroi_loads_total 1
soroi_fetch_duration_seconds_count{outcome="success"} 2
```

原始 ID、查询 key 和错误消息不会成为指标标签。每个 registry 在整个生命周期内对应一个
Client；替换 Client 时应使用新 registry 或明确的应用计数重置策略。示例只导出少量查询指标，
不覆盖全部 soroi、进程或运行时统计。

## 完整示例

以下程序为 `examples/service/main.go`。在安装步骤中的现有示例模块中运行；
`telemetry.go` 提供 `registerMetrics` 与 `errorStatus`，`rpc/users.go` 提供 protobuf 服务。
两者均包含在仓库源码中。`go run .` 会一起编译包内所有文件。

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

## 配置说明

| 配置 | 示例值 | 契约 |
| --- | --- | --- |
| `Options.StaleTime` | 一分钟 | 新鲜读取复用本地数据，不重置新鲜期 |
| `Options.Timeout` | 一秒 | 限制整次加载，包括 Executor 准入和 RPC |
| `ExecutionOptions.MaxConcurrent` | 16 | 当前进程内执行中的回调上限 |
| `ExecutionOptions.MaxQueued` | 0 | 容量不足直接拒绝，避免堆积队列 |
| `-listen` | 空 | 执行两次 HTTP 读取后退出；指定地址后持续服务 |
| `-traces` | false | 显式启用向 stderr 导出控制台 span |

容量值仅用于演示，不代表生产环境容量建议。

## 错误处理与常见问题

| 结果 | HTTP 映射 | 调用方处理 |
| --- | --- | --- |
| `ErrQueueFull` | 503 | 执行业务过载策略；soroi 不重试准入失败 |
| Context 或 RPC 截止时间到达 | 504 | 检查后端延迟和整次加载期限 |
| Context 或 RPC 取消 | 仍可发送响应时为 408 | 停止请求所属工作 |
| gRPC NotFound | 404 | 处理资源不存在 |
| 其他 RPC 失败 | 502 | 检查 trace 中记录的错误；HTTP 响应不包含内部错误 |

数据源保留原始错误。HTTP 映射不增加重试，也不将失败转换为成功缓存的资料。
示例未启用自动查询重试，也未安装应用级 gRPC 重试策略。

## 行为与限制

演示拥有并停止自身的 loopback 服务，关闭 Client、借用的 gRPC 连接和 tracer provider。
持续服务模式下 Ctrl+C 触发 HTTP 关闭。示例使用明文 loopback gRPC 和固定公开资料源；
真实应用应提供传输安全、认证、授权和业务加载函数。

诊断事件仍适用于逐 key 排查。此处指标统计查询活动和调用方延迟；存在重试或批量加载时，
`Loads` 不等于物理 RPC 次数。需要区分时，应在后端调用边界统计实际调用。

## 完整示例与相关文档

- [集成测试](../examples/service/service_test.go)：HTTP 到 gRPC 的复用、trace 传播、抓取输出和错误映射。
- [执行并发预算](execution.zh-CN.md)：准入、取消和过载契约。
- [共享结果缓存](shared-cache.zh-CN.md#缓存故障期间保护数据源)：使用相同预算 API 的 Redis 故障保护。
- [业务场景基准](business-benchmarks.zh-CN.md)：延迟总体和物理回源工作量。
