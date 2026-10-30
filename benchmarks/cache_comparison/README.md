# Cache access comparison

Compare soroi with Otter v2.4.0 using populated reads, loadable cache hits,
updates, mixed reads/writes, and eight-caller cold bursts. The independent
module keeps benchmark dependencies out of soroi's dependency-free core.

## Run

Requires Go 1.26+ because the optional real-Redis benchmark uses go-redis
v9.23.0. Install `redis-server` to include that benchmark; otherwise it skips.
The core library continues to require Go 1.22+.

From this directory:

```sh
go mod download
go test -race -run '^$' -bench . -benchtime=1x -cpu=4
go test -run '^$' -bench . -benchmem -benchtime=150ms -count=5 -cpu=1,4
```

Run before and after measurements sequentially on an otherwise quiet machine.
Do not run profiling, compilation, or another benchmark concurrently with a
measurement. The same commands work on Linux; macOS results are not evidence
of Linux performance.

## Workloads and interpretation

- `Capacity=0` disables capacity eviction. `Capacity=1024` fills both caches
  with 1,024 entries. The steady-state cases update existing keys; they do not
  measure admission quality, eviction hit ratios, or memory-pressure behavior.
- `Keys=1` reads a shared hot key; `Keys=1024` cycles through the populated
  working set with worker-specific offsets. Each case shares one cache instance.
- `Read` compares typed `soroi.Get` with `Otter.GetIfPresent`. Soroi also returns
  query state, validates the result type, and evaluates consumer freshness.
  These richer semantics are part of the measured cost, not equivalent output.
- `Fetch` compares `soroi.Fetch` with `Otter.Get`, using real one-hour clocks
  and reusable loaders. Otter statistics are enabled to match Soroi's lookup
  statistics. Both must execute zero source loads in this warm case.
- `Write` replaces existing values. `Mixed` performs one write per sixteen
  operations and uses inspection reads otherwise. Otter lookup recording is
  disabled for these inspection cases; Soroi inspection reads do not increment
  hit/miss counters. Soroi retains its documented query-state and write semantics.
- Keys and Soroi's interface representations are prepared before timing. The
  value is a fixed `uint64`, 42. Do not extrapolate these results to the cost
  of constructing keys, large objects, codecs, or application I/O.
- `ColdBurst` is eight simultaneous same-key reads from an empty cache, using
  identical SHA-256 work over 64 KiB. Each burst must execute one source load.
  Some callers may join in-flight work and others may arrive after completion;
  this is a cold burst, not proof that all eight calls joined the flight.
- `RedisBurst` replaces the SHA-256 source with one real Redis GET on loopback
  TCP. Both libraries use the same loader and must issue one physical GET per
  eight-request burst. This measures local caching over a Redis data source,
  not Soroi's Redis SharedStore or a distributed-cache feature of Otter.
- Each burst includes goroutine scheduling and source work. Cache clearing,
  fixture construction, connection readiness, and warm-up are outside timing.
  Stop/start timing around individual bursts adds harness cost outside reported
  time; it is unsuitable for estimating standalone loader latency.
- `ns/op` with `RunParallel` is elapsed time divided by total completed
  operations, not an individual request latency or a p95/p99 measurement.

The Redis fixture owns its server, selected local port, temporary directory,
client, and cleanup. It never connects to an existing Redis service.

## Recorded environment

Measurements use Apple M3 Pro, Darwin ARM64, Go 1.27.1, and CPU parallelism
fixed to one or four. Otter is pinned to v2.4.0. The Soroi baseline is commit
`2daa75e6b1c2a82b03de329305765ebd523228ae`.

Baseline and optimized binaries must be built with this same benchmark harness
and dependency graph. Record raw output with the result analysis, including
cases where Otter remains faster. Run neither binary with race instrumentation
when recording performance; use race detection for correctness checks separately.

## Profiles

For a focused cache-hit profile:

```sh
go test -run '^$' \
  -bench '^BenchmarkCacheAccess$/^Capacity=0$/^Keys=1024$/^Fetch$/^Soroi$' \
  -benchtime=3s -cpu=4 \
  -cpuprofile=/tmp/soroi-cpu.pprof \
  -mutexprofile=/tmp/soroi-mutex.pprof \
  -o /tmp/soroi-comparison.test
go tool pprof -top /tmp/soroi-comparison.test /tmp/soroi-cpu.pprof
go tool pprof -top /tmp/soroi-comparison.test /tmp/soroi-mutex.pprof
```

Use a separate allocation profile to locate heap traffic. Profiling changes
timing, so profile samples do not replace the uninstrumented comparison.
