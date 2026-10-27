# Business workload benchmarks

[简体中文](business-benchmarks.zh-CN.md)

Measure how query sharing, bounded fallback and batching affect actual source work,
caller latency and successful responses. This harness runs hot-key expiration,
Redis transport failure, downstream deadlines and bulk loading with a fixed request
population. It measures policy tradeoffs rather than cache lookup throughput.

## Installation

Requires Go 1.26+, `redis-server` on PATH and this repository checkout. Core soroi
remains dependency-free and supports Go 1.22. The benchmark's Redis dependencies
live in an independent module. No existing Redis server or credentials are used.

From the repository root:

```sh
cd benchmarks/scenarios
go mod download
go build -o /tmp/soroi-business-benchmarks .
GOMAXPROCS=4 /tmp/soroi-business-benchmarks -requests 1000 -workers 32 -max-concurrent 8
```

On Windows, choose an executable output path suitable for that platform and set
`GOMAXPROCS` using the shell's environment syntax. The Redis fixture needs a local
`redis-server` executable. Build before timing to exclude compilation.

## Usage

### Compare the same caller population

Each run emits eight JSON objects: baseline and soroi for each of four scenarios.
`configuration` records requests, workers, delays and capacities; runtime metadata
records the toolchain, platform and GOMAXPROCS. Standard error contains go-redis
connection diagnostics; standard output contains only JSON records.

| Scenario | Baseline | soroi variant |
| --- | --- | --- |
| `hot-expiry` | Every caller executes the source directly | Preinstall one key, advance an injected freshness clock to expiration once, then Fetch the same key with a one-minute freshness policy |
| `redis-outage` | Unbounded soroi loads of distinct keys with Redis fallback | The same path with one Executor, MaxConcurrent 8 and MaxQueued 0 |
| `downstream-timeout` | Distinct-key soroi loads, each with a 1 ms caller deadline | The same deadlines plus an Executor with MaxConcurrent 8 and MaxQueued 0 |
| `batching` | Each distinct key executes an individual callback | Batcher.Load collects concurrent distinct keys into bulk callbacks, with a 1 ms window and at most 64 keys |

The source models one cancellable 2 ms operation, both for individual requests and
whole batches. This models a backend with an efficient bulk API; it does not
measure any real database or claim that batch latency is independent of batch size
in production. The fixture launches a disposable Redis on a reserved loopback port,
confirms readiness, kills its own process, and retains an open client. Each outage
variant gets a new fixture and pool. Redis commands and dials are configured for
one attempt (`MaxRetries: -1`, `DialerRetries: 1`); per-store access is capped at
20 ms. It models a stopped local Redis, not packet loss, a slow server or failover.

### Interpret latency and resource fields

`all_p95_ms` and `all_p99_ms` use nearest-rank percentiles of all caller durations,
including rejection and timeout. `success_p95_ms` and `success_p99_ms` use only
successful calls; both are null when there are none. `success_rate` is successful
calls divided by all attempted calls. The counts for success, rejection, timeout
and other failure sum to the request population in every individual record.

Fixed workers dispatch calls sequentially: this is a closed-loop workload, not an
open-loop arrival-rate or production SLO test. Per-call latency starts at worker
dispatch and excludes time awaiting a worker. `elapsed_ms` includes worker startup
and source cleanup. Callbacks are drained before the next case.

`source_calls` counts physical callback entries, including failed work, and
`peak_source_calls` counts simultaneous callbacks. A caller can time out before its
callback starts, so timeouts need not equal source calls. Setup is excluded.
`allocated_bytes` is the process-wide allocation delta during workers and cleanup;
`heap_end_bytes` is the process heap at completion, not RSS or per-query memory.
`gc_count` reports collections in that interval. Resource counters include harness
and driver overhead. Baselines use the same modeled source and caller count.

## Complete example

Run three repetitions from `benchmarks/scenarios` and retain each complete output:

```sh
for run in 1 2 3; do
    GOMAXPROCS=4 /tmp/soroi-business-benchmarks -requests 1000 -workers 32 -max-concurrent 8 > "/tmp/soroi-business-$run.jsonl"
done
```

Each file contains eight records. Hot-key and batching variants should complete
all reads with fewer source calls under the default workload. Protected failure
variants bound peak source callbacks by eight while rejecting excess work.
The deliberately slower-than-deadline source normally produces no successful
responses in the timeout scenario. Timing and exact rejection counts vary.

The maintained implementation is in `benchmarks/scenarios/main.go`, `scenarios.go`
and `redis.go`. Tests cover percentile boundaries, disjoint outcome populations
and hot-key reuse without requiring a Redis server.

## Recorded measurements

Recorded on 2026-10-27 using Apple M3 Pro, Darwin arm64,
Go 1.27.1, GOMAXPROCS=4 and Redis 8.10.0. The workload uses the defaults above.
The table reports the median of three runs for each field, independently.
The [raw JSONL artifact](../benchmarks/business-darwin-arm64.jsonl) retains all
24 case records, source revision, command, environment and whole-process resource
measurements. CPU summaries cover all eight cases and owned Redis children;
`cumulative_children_peak_rss_bytes` is a running high-water mark across completed
child processes, in Darwin bytes, rather than an independent per-case RSS value.

| Scenario | Variant | Source calls | Peak source calls | Success % | Rejected | Timeouts | All P95 ms | All P99 ms | Success P99 ms | Allocated KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot-expiry | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.339 | 2.377 | 2.377 | 276.1 |
| hot-expiry | soroi | 1 | 1 | 100.0 | 0 | 0 | 0.197 | 2.668 | 2.668 | 56.9 |
| redis-outage | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.617 | 3.125 | 3.125 | 4085.0 |
| redis-outage | soroi | 123 | 8 | 12.3 | 877 | 0 | 2.371 | 2.506 | 2.637 | 1636.2 |
| downstream-timeout | baseline | 1000 | 32 | 0.0 | 0 | 1000 | 1.272 | 1.305 | n/a | 2009.3 |
| downstream-timeout | soroi | 119 | 8 | 0.0 | 871 | 129 | 1.038 | 1.177 | n/a | 1677.2 |
| batching | baseline | 1000 | 32 | 100.0 | 0 | 0 | 2.361 | 2.405 | 2.405 | 247.6 |
| batching | soroi | 32 | 1 | 100.0 | 0 | 0 | 3.643 | 4.085 | 4.085 | 540.5 |

Hot-key sharing reduces source calls even though callers joining the initial
reload still pay its latency. The batching collection window increases caller
latency in this workload while reducing physical calls. Under Redis failure,
protected source concurrency stays at eight because excess requests are rejected;
this is load protection, not an increase in successful responses. No timeout
variant recovers a source whose service time exceeds its deadline.

## Configuration

| Flag | Default | Constraint |
| --- | --- | --- |
| `-requests` | 1000 | Positive caller count per case |
| `-workers` | 32 | Positive closed-loop concurrency |
| `-source-delay` | 2ms | Nonnegative individual or bulk source service time |
| `-timeout` | 1ms | Positive timeout-case caller deadline |
| `-max-concurrent` | 8 | Positive protected source concurrency; queue is zero |
| `-batch-size` | 64 | Positive maximum unique keys per batch |
| `-batch-wait` | 1ms | Nonnegative collection window |

## Error handling and common problems

| Symptom | Cause to check | Action |
| --- | --- | --- |
| Cannot start Redis | Missing executable or a reserved port was taken | Install Redis or rerun after checking local listeners |
| Redis diagnostics on stderr | Expected reconnection failure after fixture shutdown | Retain stdout separately; do not merge logs into JSONL |
| Low success rate in protected cases | Zero queue rejects overload by design | Inspect successes and rejected counts together with latency |
| No success percentile | No request succeeded | Interpret null as no sample, never zero-latency success |

## Behavior and limitations

This is a synthetic single-process workload on one machine. It does not prove
cluster-wide admission, source correctness, production capacity, failover behavior
or stable tail-latency improvements. Repeat on deployment hardware with realistic
key distributions, bulk costs, source latency and failure modes before making
performance claims. The default order is fixed; compare repeated samples and
account for process warmup, allocation history and scheduler noise.

## Complete examples and related guides

- [HTTP and gRPC integration](service-integrations.en-US.md): request tracing and Prometheus metrics.
- [Shared cache failure protection](shared-cache.en-US.md#protect-the-source-during-cache-failures): existing API composition and limits.
- [Automatic batching](batching.en-US.md): per-key outcomes and callback capacity.
