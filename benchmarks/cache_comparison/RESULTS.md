# Cache comparison results

Recorded on October 31, 2026: Apple M3 Pro, Darwin ARM64, Go 1.27.1,
Otter v2.4.0, Redis 8.10.0. The Soroi baseline is
`2daa75e6b1c2a82b03de329305765ebd523228ae`. See [methodology](README.md),
[baseline output](before-darwin-arm64.txt), and [optimized output](after-darwin-arm64.txt).

Both binaries were built with the same benchmark sources and dependency graph.
Only the Soroi source replacement differed. Measurements ran sequentially,
without race instrumentation or concurrent profiling/builds, with five samples
per case at `-benchtime=150ms -cpu=1,4`. Tables show medians, not a statistical
significance claim or request-tail latency. All 72 named CPU-specific cases have
five samples in each result file.

The optimized core is commit `12fbff066a5dc33c967a273cfe92315934a4f1b1`.

## Steady-state results

Values below are ns/op for a 1,024-key working set. CPU count is the Go
parallelism setting. All measured warm cases completed without source calls.

| Capacity | CPUs | Operation | Soroi before | Soroi after | Otter after | Soroi time reduction |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Unbounded | 1 | Read | 243.00 | 129.20 | 33.48 | 46.8% |
| Unbounded | 1 | Fetch | 204.40 | 148.40 | 46.90 | 27.4% |
| Unbounded | 1 | Write | 189.00 | 122.20 | 91.45 | 35.3% |
| Unbounded | 1 | Mixed | 242.90 | 128.50 | 37.58 | 47.1% |
| Unbounded | 4 | Read | 366.40 | 62.12 | 8.44 | 83.0% |
| Unbounded | 4 | Fetch | 293.60 | 87.16 | 12.18 | 70.3% |
| Unbounded | 4 | Write | 282.60 | 221.30 | 31.51 | 21.7% |
| Unbounded | 4 | Mixed | 361.00 | 191.70 | 10.96 | 46.9% |
| 1,024 | 1 | Read | 273.90 | 186.60 | 33.08 | 31.9% |
| 1,024 | 1 | Fetch | 241.10 | 203.30 | 46.80 | 15.7% |
| 1,024 | 1 | Write | 216.90 | 177.90 | 90.99 | 18.0% |
| 1,024 | 1 | Mixed | 270.30 | 186.60 | 37.03 | 31.0% |
| 1,024 | 4 | Read | 406.30 | 280.20 | 8.53 | 31.0% |
| 1,024 | 4 | Fetch | 353.90 | 323.20 | 12.44 | 8.7% |
| 1,024 | 4 | Write | 322.30 | 265.70 | 31.49 | 17.6% |
| 1,024 | 4 | Mixed | 398.10 | 280.70 | 11.02 | 29.5% |

Soroi remains slower than Otter in these pure cache-access cases. In particular,
bounded reads still update exact LRU recency under the exclusive Client lock.
Soroi inspection returns typed query snapshots, including freshness and loading
state, while Otter inspection returns a value and presence flag. The Fetch cases
are the closer loading-cache comparison; both include lookup statistics.

For the unbounded, four-CPU, 1,024-key Fetch case, heap traffic fell from
25 B/op and 2 allocs/op to 0 B/op and 0 allocs/op. Inspection and update cases
also reached zero per-operation allocation for the prepared scalar keys used here.

## Cold bursts and Redis

Each operation below is an entire eight-caller burst, in microseconds.
Each sample asserted exactly one physical source invocation per burst.

| Source | CPUs | Soroi before | Soroi after | Otter after |
| --- | ---: | ---: | ---: | ---: |
| SHA-256, 64 KiB | 1 | 37.415 | 36.200 | 29.216 |
| SHA-256, 64 KiB | 4 | 40.121 | 37.887 | 37.563 |
| Redis GET, loopback TCP | 1 | 29.478 | 27.718 | 27.899 |
| Redis GET, loopback TCP | 4 | 34.552 | 32.771 | 32.079 |

Redis results are close and affected by scheduling and transport variation.
The slight one-CPU median advantage is not evidence that Soroi is faster than
Otter. This benchmark uses the same Redis-backed application loader for both
libraries; it does not measure Soroi SharedStore scripts, codecs, remote
invalidation, or a built-in Redis adapter in Otter.

## Changes and evidence

- Keep typed ordinary fetchers in an internal interface and erase the result
  only on actual loader invocation, removing the per-lookup adapter closure.
- Use type-level comparability checks for scalar keys. Composite keys still
  receive value-level validation, and non-reflexive keys remain rejected.
  Allocation profiling on this Go version attributed the previous small
  allocation on each scalar lookup to `reflect.Value.Comparable`.
- Project already-read entries directly into snapshots, avoiding duplicate
  age checks, map lookups, and freshness clock reads.
- Skip inactive-retention and maximum-age bookkeeping when disabled.
- Share the read lock for inspection when the default clock is used and
  capacity/age/inactive-retention policies cannot mutate state. Fresh Fetch
  hits use this path only without tag registration, shared-store binding,
  forced refresh, or event delivery. Mutating paths retain exclusive locking.
- Count shared-lock hits atomically; Stats takes the exclusive lock and
  incorporates those hits into its coherent snapshot. Custom-clock callbacks
  keep serialized execution.
- Install Set values and type bindings together, removing the redundant
  intermediate map write.

The optimized four-CPU Fetch profile measured approximately 87 ns/op with
zero allocations. Around 27% of sampled CPU time was in reader-lock atomic
updates and 17% in atomic hit-counter updates. Time-source reads and request
preparation also remain visible. The mutex profile no longer attributed
blocking delay to Client.fetch in this unbounded warm-hit case. Sampling
percentages describe this one profiled run and are not universal cost ratios.

## Validation and limits

- Root checks: `task fmt`, `task lint`, `task test`, and `task build`.
- Comparison module: `go vet ./...`, repository-configured golangci-lint, and
  race-instrumented one-iteration runs of every benchmark, including Redis.
- Regression coverage: zero-allocation cached Fetch, exact hit counts during
  concurrent reads/writes/Stats, and retained cache-hit diagnostic events.
- Existing race-enabled tests cover type validation, nil results, cancellation,
  sharing, expiry, GC, capacity eviction, subscriptions, mutations, and pagination.
- Linux binaries were cross-built for amd64 and arm64; no Linux runtime or
  Linux kernel/network performance measurements were available.
- Prepared small values, fully populated caches, and controlled cold bursts
  do not establish eviction quality, p95/p99 latency, memory-budget behavior,
  production Redis performance, or a general cross-library ranking.

## Remaining work toward the speed target

The goal of beating Otter on pure cache access has not been reached. A further
design must address shared reader/statistics cache-line contention and bounded
LRU serialization rather than merely renaming APIs or disabling correctness
checks. Candidate work includes measured partitioning of query state and
statistics, and a carefully specified typed value-read path. Changes must retain
subscription ownership, invalidation ordering, per-consumer freshness, and
coherent statistics, with explicit CPU/memory tradeoffs.

Reproduce the suite on an actual Linux host before choosing Linux-specific
synchronization, allocator, or Redis transport changes. No kernel parameters
were changed, and the Redis adapter was not modified based on a source-loader
benchmark.
