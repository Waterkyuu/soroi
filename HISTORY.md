# Performance history

The current cache uses a bucket read lock. With the default clock, a live MaxAge read can share that lock. For GCTime, each idle entry owns its cleanup task. A hit records the latest elapsed time and reuses the timer. The two later GCTime snapshot experiments were retired. Their source, read gains and write costs are recorded here.

- [Earlier cache and concurrency work](#earlier-cache-and-concurrency-work)
- [Changes to value reads and idle cleanup](#changes-to-value-reads-and-idle-cleanup)
- [24-case comparison across 14 implementations](#24-case-comparison)
- [Original measurements, including alternating writes and reads](#original-measurements)
- [Reproduce the 24 cases](#reproduction)

## Earlier cache and concurrency work

These runs predate the public value-only Get in `0250a1f`. The old Get returned query state. Fetch, group lifecycles and batches also measured different operations. The figures below retain each study's own workload, baseline and units.

| Change | Original comparison and outcome |
| --- | --- |
| Read freshness directly from the cache entry (`d556311`) | Four callers completed 100 same-key cached requests in 34.27 → 25.64 µs/batch. For a cold first load followed by reuse, 67.30 → 58.08 µs/batch. Restoring the previous code yielded 35.03 and 67.87 µs/batch. [Harness, controls and samples](benchmarks/benchmarks.en-US.md#investigation-same-key-concurrent-read-costs) |
| Reduce hit allocations, allow shared read locking and skip disabled retention work (`1629274`, `12fbff0`, followed by the disabled-capacity shortcut in `8c81e52`) | In the paired `2daa75e` → `12fbff0` run, unbounded 1,024-key reads at four workers fell from 366.40 to 62.12 ns/op. Fetch fell from 293.60 to 87.16 and Write from 282.60 to 221.30 ns/op. Bounded reads were 406.30 → 280.20 ns/op. The pair measures the combined change through `12fbff0`. [All capacity, mixed and cold-burst results](benchmarks/cache_comparison/RESULTS.md) |
| Buffer LRU recency updates (opt-in prototype `e7d0c9f`) | With four workers, the original strict implementation and the buffered candidate measured 286.10 → 79.71 ns/op for Read, 333.60 → 97.33 for Fetch and 285.10 → 307.80 for mixed traffic. This experiment enabled capacity and relaxed recency ordering. [Original study](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/169e3e8/实验/批量排名/实施结果.md) |
| Reuse stable entries (`8e4dc0c`), then route a client's keys to 64 buckets (`df18561`) | From the read-fix build to 64 buckets, eight-worker distributed snapshot Get fell from 107.85 to 31.71 ns/op, Set from 276.00 to 49.03, and warm Fetch from 161.55 to 32.61. Eight-worker hot-key Get rose from 106.35 to 127.25 ns/op. [Source, samples and workload definitions](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/6925f1f/experiments/reused_bucket_routing/RESULTS.md) |
| Restrict grouped writes to connected buckets and consolidate topology allocations (`fcee6a3`, `e7935ff`) | An eight-worker grouped four-call workflow fell from 3,440.50 to 383.50 ns/workflow. At one worker, creating and closing a group alongside 32 existing groups rose from 2,580 to 6,275.5 ns/cycle. [Paired results and memory costs](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/6925f1f/experiments/group_lock_scope/RESULTS.md) |
| Issue data revisions inside the bucket (`e9117cc`) | Eight-worker distributed Set took 26.77% less time. The four-step workflow without subscriptions took 8.07% less time. [Full measurements](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/8cb5364/experiments/bucket_local_revisions/RESULTS.md) |
| Reuse the validated entry on Get/Fetch (`21d64db`) and within each Update lock phase (`f81aaad`) | The read recheck measured Get −7.36%, Fetch −3.28% and Set +0.20% at eight workers. The separate Update trial measured −26.45% at one worker and −19.52% at eight. [Read recheck](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/04db09c/experiments/single_lookup_recheck/RESULTS.md) · [Update results](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/08687b2/experiments/update_entry_reuse/RESULTS.md) |
| Incrementally merge new group connections (`6b4375d`) | At CPU=8, serial group creation/closure with 32 existing groups took 23.38% less time. In a separate eight-worker recheck, Set was 38.365 → 37.590 ns/op, Get 29.535 → 29.515, Fetch 33.655 → 33.605 and the grouped workflow 365.450 → 371.050 ns/workflow. Its paired interval extended to +6.19%. [Study and samples](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/36d6d38/experiments/admission_guard_recheck/RESULTS.md) |
| Earlier discarded designs on the snapshot-returning API | A lazy read view improved hot Get by 85.67% but increased the cost of one write followed by one read by 227.77%. Two reader lanes improved hot Get by 33.64% with Set +19.68%. Maintaining a reverse group index increased the fully connected lifecycle cost by 96.34%. [Methods, results and source revisions](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/631443b/HANDOFF.md) |
| Publish or reuse a snapshot record per key (`4551ebf`, then `6b0508b` in diagnostics) | Eight-worker same-key snapshot Get was 127.85 → 11.72 ns/op with a new record per write and 125.60 → 14.81 with record reuse, each against its own control. Distributed Set regressed by 45.84% / 46.43% at one / eight workers in the first trial and by 14.02% / 9.25% with reuse. [New record](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/c4a6711/experiments/eager_read_snapshot/RESULTS.md) · [Reused record](https://github.com/Waterkyuu/soroi-performance-diagnostics/blob/8af80ec/experiments/reused_read_snapshot/RESULTS.md) |
| Combined release comparison (`80738c3` → `d8fa236`) | Eight-worker Get 374.75 → 28.38 ns/op, Set 305.00 → 36.16 ns/op, grouped workflow 3,328.00 → 381.30 ns/workflow. This includes multiple optimizations. [Workload, intervals and raw evidence](README.md#eight-worker-cached-operations) |

## Changes to value reads and idle cleanup

| Source | What changed | Disposition |
| --- | --- | --- |
| `0250a1f` | Get returns the value and a hit flag. GetState provides full query state. | First revision in the 24-case comparison. |
| `e8fa553` | Get shares the bucket read lock when a value is within MaxAge. An expired read checks again under the write lock. | Retained. |
| `938547f` | GCTime hits advance an exact per-key timestamp. The existing timer checks the latest deadline on wakeup. | Retained. |
| `elapsed`, `lookup`, `combined`, `striped` | Isolate direct monotonic elapsed time, move the cleanup task onto the entry, combine both, then split timestamp writes across four padded slots. | Prototypes based on `938547f`. The four-slot version used extra memory per timer. |
| `two`, `four` | Split each bucket's reader registration across two or four padded counters. Writers acquire every shard. | Prototypes based on `938547f`. Shard selection used private Go runtime functions. |
| `935be04` → `74c5b70` | Each eligible Set replaces one key's immutable value record. The first version handles default and MaxAge reads. The second extends the fast path to exact GCTime renewal. | Separate experimental branch, based on `938547f`. |
| `febe4c7` | Store the cleanup task on the entry and use `time.Since` with the default clock. This version puts the task pointer at the end of the entry. | Retained. Its production Go source matches `527ea6d`. |
| `018eadd` | After a write, the first eligible Get builds a snapshot. Later Gets reuse it. Another Set invalidates it. | Retired in `22c2300`. |
| `5604a00` | Every eligible Set creates a 32-byte snapshot. The next Get can immediately use it. | Retired in `a78b555`. Both later snapshot trials start from optimized GCTime. |

## 24-case comparison

Measured on 2026-11-19 with Go 1.27.1, macOS 14.6/arm64 and an Apple M3 Pro. Each row measures one successful untyped `client.Get(key)` or one overwrite using `soroi.Set`. Workers either divide 1,024 naturally hashed strings into disjoint stripes or share a single key. All keys are populated and read before timing. Writes alternate the prepared integers 42 and 43 on successive passes. Readback follows the timed writes. Policies use defaults, MaxAge alone at one hour, or GCTime alone at one hour.

Each cell shows **ns/op / B/op / allocs/op**. The percentage is the change in ns/op from the baseline identified for that column. Negative values mean less elapsed time per completed operation. Parallel ns/op measures aggregate throughput. Memory figures use Go's integer benchmem counters. Each figure is the median of three 100 ms samples. Binaries ran sequentially, with the source order reversed in round two. Small differences in these short runs can be noise. [Measurement criteria](AGENTS.md#get-and-set-baseline-measurement-criteria) · [Harness](cache_baseline_benchmark_test.go) · [All 1,008 samples](benchmarks/cache_comparison/history-matrix-darwin-arm64.txt)

### Retained changes

Read from left to right. Each percentage compares with the immediately preceding revision.

| Operation | Keys | Workers | Policy | **Value Get** | **Shared MaxAge lock** | **Reuse GC timer** | **Task on entry + time.Since** |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| Get | 1,024 | 1 | Default | 29.85 / 0 / 0 | 32.11 / 0 / 0 (+7.6%) | 33.2 / 0 / 0 (+3.4%) | 31.24 / 0 / 0 (-5.9%) |
| Get | 1,024 | 8 | Default | 12.3 / 0 / 0 | 12.59 / 0 / 0 (+2.4%) | 12.83 / 0 / 0 (+1.9%) | 12.47 / 0 / 0 (-2.8%) |
| Get | 1,024 | 1 | MaxAge | 99.79 / 0 / 0 | 70.2 / 0 / 0 (-29.7%) | 69.73 / 0 / 0 (-0.7%) | 68.42 / 0 / 0 (-1.9%) |
| Get | 1,024 | 8 | MaxAge | 37.06 / 0 / 0 | 20.04 / 0 / 0 (-45.9%) | 18.58 / 0 / 0 (-7.3%) | 20.94 / 0 / 0 (+12.7%) |
| Get | 1,024 | 1 | GCTime | 434.2 / 222 / 4 | 421.4 / 225 / 4 (-2.9%) | 83.05 / 0 / 0 (-80.3%) | 48.73 / 0 / 0 (-41.3%) |
| Get | 1,024 | 8 | GCTime | 169.6 / 208 / 4 | 151.3 / 208 / 4 (-10.8%) | 25.85 / 0 / 0 (-82.9%) | 17.55 / 0 / 0 (-32.1%) |
| Get | 1 | 1 | Default | 25.93 / 0 / 0 | 25.98 / 0 / 0 (+0.2%) | 27.06 / 0 / 0 (+4.2%) | 25.73 / 0 / 0 (-4.9%) |
| Get | 1 | 8 | Default | 102.6 / 0 / 0 | 96.88 / 0 / 0 (-5.6%) | 94.55 / 0 / 0 (-2.4%) | 102.9 / 0 / 0 (+8.8%) |
| Get | 1 | 1 | MaxAge | 90.92 / 0 / 0 | 56.39 / 0 / 0 (-38.0%) | 57.17 / 0 / 0 (+1.4%) | 56.2 / 0 / 0 (-1.7%) |
| Get | 1 | 8 | MaxAge | 233.6 / 0 / 0 | 107.4 / 0 / 0 (-54.0%) | 111.6 / 0 / 0 (+3.9%) | 117.2 / 0 / 0 (+5.0%) |
| Get | 1 | 1 | GCTime | 311.7 / 208 / 4 | 300.5 / 208 / 4 (-3.6%) | 65.43 / 0 / 0 (-78.2%) | 42.23 / 0 / 0 (-35.5%) |
| Get | 1 | 8 | GCTime | 403 / 208 / 4 | 403.1 / 208 / 4 (+0.0%) | 133.2 / 0 / 0 (-67.0%) | 128.3 / 0 / 0 (-3.7%) |
| Set | 1,024 | 1 | Default | 82.67 / 0 / 0 | 84.68 / 0 / 0 (+2.4%) | 84.1 / 0 / 0 (-0.7%) | 83.24 / 0 / 0 (-1.0%) |
| Set | 1,024 | 8 | Default | 33.7 / 0 / 0 | 40.69 / 0 / 0 (+20.7%) | 37.69 / 0 / 0 (-7.4%) | 36.44 / 0 / 0 (-3.3%) |
| Set | 1,024 | 1 | MaxAge | 88.4 / 0 / 0 | 88.21 / 0 / 0 (-0.2%) | 94.34 / 0 / 0 (+6.9%) | 87.33 / 0 / 0 (-7.4%) |
| Set | 1,024 | 8 | MaxAge | 42.99 / 0 / 0 | 38.5 / 0 / 0 (-10.4%) | 49.1 / 0 / 0 (+27.5%) | 35.31 / 0 / 0 (-28.1%) |
| Set | 1,024 | 1 | GCTime | 503.2 / 208 / 4 | 525.5 / 208 / 4 (+4.4%) | 152.6 / 0 / 0 (-71.0%) | 135.4 / 0 / 0 (-11.3%) |
| Set | 1,024 | 8 | GCTime | 162.5 / 208 / 4 | 321.1 / 208 / 4 (+97.6%) | 54.07 / 0 / 0 (-83.2%) | 44.26 / 0 / 0 (-18.1%) |
| Set | 1 | 1 | Default | 77.38 / 0 / 0 | 83.13 / 0 / 0 (+7.4%) | 77.24 / 0 / 0 (-7.1%) | 75.01 / 0 / 0 (-2.9%) |
| Set | 1 | 8 | Default | 213.1 / 0 / 0 | 215.8 / 0 / 0 (+1.3%) | 220.3 / 0 / 0 (+2.1%) | 221.8 / 0 / 0 (+0.7%) |
| Set | 1 | 1 | MaxAge | 78.62 / 0 / 0 | 79.06 / 0 / 0 (+0.6%) | 79.81 / 0 / 0 (+0.9%) | 78.57 / 0 / 0 (-1.6%) |
| Set | 1 | 8 | MaxAge | 227.5 / 0 / 0 | 219.2 / 0 / 0 (-3.6%) | 222.1 / 0 / 0 (+1.3%) | 224.1 / 0 / 0 (+0.9%) |
| Set | 1 | 1 | GCTime | 384.2 / 208 / 4 | 411.5 / 208 / 4 (+7.1%) | 134.3 / 0 / 0 (-67.4%) | 105.8 / 0 / 0 (-21.2%) |
| Set | 1 | 8 | GCTime | 449.8 / 208 / 4 | 450.2 / 208 / 4 (+0.1%) | 230.5 / 0 / 0 (-48.8%) | 233.7 / 0 / 0 (+1.4%) |

### GC task and clock experiments

Elapsed, Lookup and Combined compare with the indexed task in `938547f`. Four slots compares with Combined, which has one slot. Combined places the task pointer at the start of the entry.

| Operation | Keys | Workers | Policy | **Indexed task** | **Elapsed** | **Lookup** | **Combined** | **Four slots** |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Get | 1,024 | 1 | Default | 33.2 / 0 / 0 | 31.33 / 0 / 0 (-5.6%) | 31.3 / 0 / 0 (-5.7%) | 32.05 / 0 / 0 (-3.5%) | 32.61 / 0 / 0 (+1.7%) |
| Get | 1,024 | 8 | Default | 12.83 / 0 / 0 | 12.25 / 0 / 0 (-4.5%) | 12.35 / 0 / 0 (-3.7%) | 13.46 / 0 / 0 (+4.9%) | 13.59 / 0 / 0 (+1.0%) |
| Get | 1,024 | 1 | MaxAge | 69.73 / 0 / 0 | 67.77 / 0 / 0 (-2.8%) | 68.48 / 0 / 0 (-1.8%) | 70.08 / 0 / 0 (+0.5%) | 72.12 / 0 / 0 (+2.9%) |
| Get | 1,024 | 8 | MaxAge | 18.58 / 0 / 0 | 18.21 / 0 / 0 (-2.0%) | 18.62 / 0 / 0 (+0.2%) | 18.71 / 0 / 0 (+0.7%) | 18.89 / 0 / 0 (+1.0%) |
| Get | 1,024 | 1 | GCTime | 83.05 / 0 / 0 | 65.09 / 0 / 0 (-21.6%) | 68.21 / 0 / 0 (-17.9%) | 50.08 / 0 / 0 (-39.7%) | 62.32 / 0 / 0 (+24.4%) |
| Get | 1,024 | 8 | GCTime | 25.85 / 0 / 0 | 21.03 / 0 / 0 (-18.6%) | 20.63 / 0 / 0 (-20.2%) | 17.88 / 0 / 0 (-30.8%) | 17 / 0 / 0 (-4.9%) |
| Get | 1 | 1 | Default | 27.06 / 0 / 0 | 25.82 / 0 / 0 (-4.6%) | 25.78 / 0 / 0 (-4.7%) | 26.75 / 0 / 0 (-1.1%) | 27.14 / 0 / 0 (+1.5%) |
| Get | 1 | 8 | Default | 94.55 / 0 / 0 | 95.82 / 0 / 0 (+1.3%) | 101.6 / 0 / 0 (+7.5%) | 94.64 / 0 / 0 (+0.1%) | 95.81 / 0 / 0 (+1.2%) |
| Get | 1 | 1 | MaxAge | 57.17 / 0 / 0 | 57.25 / 0 / 0 (+0.1%) | 57.29 / 0 / 0 (+0.2%) | 58.64 / 0 / 0 (+2.6%) | 59.78 / 0 / 0 (+1.9%) |
| Get | 1 | 8 | MaxAge | 111.6 / 0 / 0 | 114.8 / 0 / 0 (+2.9%) | 109.5 / 0 / 0 (-1.9%) | 111.4 / 0 / 0 (-0.2%) | 126.3 / 0 / 0 (+13.4%) |
| Get | 1 | 1 | GCTime | 65.43 / 0 / 0 | 50.56 / 0 / 0 (-22.7%) | 57.26 / 0 / 0 (-12.5%) | 43 / 0 / 0 (-34.3%) | 43.88 / 0 / 0 (+2.0%) |
| Get | 1 | 8 | GCTime | 133.2 / 0 / 0 | 124.1 / 0 / 0 (-6.8%) | 130 / 0 / 0 (-2.4%) | 130.4 / 0 / 0 (-2.1%) | 131.8 / 0 / 0 (+1.1%) |
| Set | 1,024 | 1 | Default | 84.1 / 0 / 0 | 84.1 / 0 / 0 (+0.0%) | 82.67 / 0 / 0 (-1.7%) | 85.02 / 0 / 0 (+1.1%) | 84.76 / 0 / 0 (-0.3%) |
| Set | 1,024 | 8 | Default | 37.69 / 0 / 0 | 36.29 / 0 / 0 (-3.7%) | 35.95 / 0 / 0 (-4.6%) | 39.83 / 0 / 0 (+5.7%) | 51.49 / 0 / 0 (+29.3%) |
| Set | 1,024 | 1 | MaxAge | 94.34 / 0 / 0 | 92.79 / 0 / 0 (-1.6%) | 86.76 / 0 / 0 (-8.0%) | 88.53 / 0 / 0 (-6.2%) | 90.26 / 0 / 0 (+2.0%) |
| Set | 1,024 | 8 | MaxAge | 49.1 / 0 / 0 | 36.87 / 0 / 0 (-24.9%) | 37.42 / 0 / 0 (-23.8%) | 35.36 / 0 / 0 (-28.0%) | 42.27 / 0 / 0 (+19.5%) |
| Set | 1,024 | 1 | GCTime | 152.6 / 0 / 0 | 133.3 / 0 / 0 (-12.6%) | 133.3 / 0 / 0 (-12.6%) | 120.5 / 0 / 0 (-21.0%) | 126.8 / 0 / 0 (+5.2%) |
| Set | 1,024 | 8 | GCTime | 54.07 / 0 / 0 | 48.28 / 0 / 0 (-10.7%) | 46.2 / 0 / 0 (-14.6%) | 44.51 / 0 / 0 (-17.7%) | 47.04 / 0 / 0 (+5.7%) |
| Set | 1 | 1 | Default | 77.24 / 0 / 0 | 74.99 / 0 / 0 (-2.9%) | 76.8 / 0 / 0 (-0.6%) | 75.66 / 0 / 0 (-2.0%) | 76.82 / 0 / 0 (+1.5%) |
| Set | 1 | 8 | Default | 220.3 / 0 / 0 | 217.4 / 0 / 0 (-1.3%) | 213.8 / 0 / 0 (-3.0%) | 197.9 / 0 / 0 (-10.2%) | 215.7 / 0 / 0 (+9.0%) |
| Set | 1 | 1 | MaxAge | 79.81 / 0 / 0 | 80.19 / 0 / 0 (+0.5%) | 78.07 / 0 / 0 (-2.2%) | 79.65 / 0 / 0 (-0.2%) | 83.32 / 0 / 0 (+4.6%) |
| Set | 1 | 8 | MaxAge | 222.1 / 0 / 0 | 217 / 0 / 0 (-2.3%) | 224.7 / 0 / 0 (+1.2%) | 214 / 0 / 0 (-3.6%) | 227.1 / 0 / 0 (+6.1%) |
| Set | 1 | 1 | GCTime | 134.3 / 0 / 0 | 119.4 / 0 / 0 (-11.1%) | 122.6 / 0 / 0 (-8.7%) | 111.5 / 0 / 0 (-17.0%) | 112 / 0 / 0 (+0.4%) |
| Set | 1 | 8 | GCTime | 230.5 / 0 / 0 | 254 / 0 / 0 (+10.2%) | 274.9 / 0 / 0 (+19.3%) | 229.1 / 0 / 0 (-0.6%) | 254.8 / 0 / 0 (+11.2%) |

### Reader counters and the first snapshot implementation

Both reader-counter variants and both write-time snapshot versions compare with `938547f`. The snapshot columns identify the last eligible policy added to each version.

| Operation | Keys | Workers | Policy | **One counter** | **Two counters** | **Four counters** | **Through MaxAge** | **Through GCTime** |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Get | 1,024 | 1 | Default | 33.2 / 0 / 0 | 44.32 / 0 / 0 (+33.5%) | 32.14 / 0 / 0 (-3.2%) | 31.69 / 0 / 0 (-4.5%) | 35.98 / 0 / 0 (+8.4%) |
| Get | 1,024 | 8 | Default | 12.83 / 0 / 0 | 15.71 / 0 / 0 (+22.4%) | 9.307 / 0 / 0 (-27.5%) | 4.92 / 0 / 0 (-61.7%) | 5.47 / 0 / 0 (-57.4%) |
| Get | 1,024 | 1 | MaxAge | 69.73 / 0 / 0 | 75.17 / 0 / 0 (+7.8%) | 71.32 / 0 / 0 (+2.3%) | 59.54 / 0 / 0 (-14.6%) | 63.08 / 0 / 0 (-9.5%) |
| Get | 1,024 | 8 | MaxAge | 18.58 / 0 / 0 | 18.7 / 0 / 0 (+0.6%) | 15.77 / 0 / 0 (-15.1%) | 10.35 / 0 / 0 (-44.3%) | 11.34 / 0 / 0 (-39.0%) |
| Get | 1,024 | 1 | GCTime | 83.05 / 0 / 0 | 81.89 / 0 / 0 (-1.4%) | 82.57 / 0 / 0 (-0.6%) | 79.56 / 0 / 0 (-4.2%) | 62.06 / 0 / 0 (-25.3%) |
| Get | 1,024 | 8 | GCTime | 25.85 / 0 / 0 | 23.13 / 0 / 0 (-10.5%) | 20.99 / 0 / 0 (-18.8%) | 23 / 0 / 0 (-11.0%) | 10.81 / 0 / 0 (-58.2%) |
| Get | 1 | 1 | Default | 27.06 / 0 / 0 | 27.69 / 0 / 0 (+2.3%) | 28.6 / 0 / 0 (+5.7%) | 21.15 / 0 / 0 (-21.8%) | 25.26 / 0 / 0 (-6.7%) |
| Get | 1 | 8 | Default | 94.55 / 0 / 0 | 61.79 / 0 / 0 (-34.6%) | 33.3 / 0 / 0 (-64.8%) | 3.508 / 0 / 0 (-96.3%) | 5.456 / 0 / 0 (-94.2%) |
| Get | 1 | 1 | MaxAge | 57.17 / 0 / 0 | 63.11 / 0 / 0 (+10.4%) | 60.93 / 0 / 0 (+6.6%) | 51.49 / 0 / 0 (-9.9%) | 55.43 / 0 / 0 (-3.0%) |
| Get | 1 | 8 | MaxAge | 111.6 / 0 / 0 | 78.46 / 0 / 0 (-29.7%) | 42.95 / 0 / 0 (-61.5%) | 8.707 / 0 / 0 (-92.2%) | 10.37 / 0 / 0 (-90.7%) |
| Get | 1 | 1 | GCTime | 65.43 / 0 / 0 | 69.25 / 0 / 0 (+5.8%) | 69.74 / 0 / 0 (+6.6%) | 66.48 / 0 / 0 (+1.6%) | 64.16 / 0 / 0 (-1.9%) |
| Get | 1 | 8 | GCTime | 133.2 / 0 / 0 | 107.1 / 0 / 0 (-19.6%) | 90.51 / 0 / 0 (-32.0%) | 130.2 / 0 / 0 (-2.3%) | 82.23 / 0 / 0 (-38.3%) |
| Set | 1,024 | 1 | Default | 84.1 / 0 / 0 | 88.01 / 0 / 0 (+4.6%) | 99.04 / 0 / 0 (+17.8%) | 126.1 / 64 / 1 (+49.9%) | 130 / 64 / 1 (+54.6%) |
| Set | 1,024 | 8 | Default | 37.69 / 0 / 0 | 47.67 / 0 / 0 (+26.5%) | 56.14 / 0 / 0 (+49.0%) | 52.14 / 64 / 1 (+38.3%) | 57.79 / 64 / 1 (+53.3%) |
| Set | 1,024 | 1 | MaxAge | 94.34 / 0 / 0 | 94.53 / 0 / 0 (+0.2%) | 101.1 / 0 / 0 (+7.2%) | 132.3 / 64 / 1 (+40.2%) | 130.2 / 64 / 1 (+38.0%) |
| Set | 1,024 | 8 | MaxAge | 49.1 / 0 / 0 | 52.15 / 0 / 0 (+6.2%) | 56.5 / 0 / 0 (+15.1%) | 101.1 / 64 / 1 (+105.9%) | 61.86 / 64 / 1 (+26.0%) |
| Set | 1,024 | 1 | GCTime | 152.6 / 0 / 0 | 164.2 / 0 / 0 (+7.6%) | 166.3 / 0 / 0 (+9.0%) | 157.1 / 0 / 0 (+2.9%) | 207.6 / 64 / 1 (+36.0%) |
| Set | 1,024 | 8 | GCTime | 54.07 / 0 / 0 | 74.23 / 0 / 0 (+37.3%) | 70.16 / 0 / 0 (+29.8%) | 60.08 / 0 / 0 (+11.1%) | 79.87 / 64 / 1 (+47.7%) |
| Set | 1 | 1 | Default | 77.24 / 0 / 0 | 81.81 / 0 / 0 (+5.9%) | 88.07 / 0 / 0 (+14.0%) | 108.6 / 64 / 1 (+40.6%) | 114 / 64 / 1 (+47.6%) |
| Set | 1 | 8 | Default | 220.3 / 0 / 0 | 226.6 / 0 / 0 (+2.9%) | 184.7 / 0 / 0 (-16.2%) | 239.2 / 64 / 1 (+8.6%) | 201.7 / 64 / 1 (-8.4%) |
| Set | 1 | 1 | MaxAge | 79.81 / 0 / 0 | 89.58 / 0 / 0 (+12.2%) | 93.65 / 0 / 0 (+17.3%) | 111.7 / 64 / 1 (+40.0%) | 126.1 / 64 / 1 (+58.0%) |
| Set | 1 | 8 | MaxAge | 222.1 / 0 / 0 | 236.2 / 0 / 0 (+6.3%) | 193.4 / 0 / 0 (-12.9%) | 208.7 / 64 / 1 (-6.0%) | 212.6 / 64 / 1 (-4.3%) |
| Set | 1 | 1 | GCTime | 134.3 / 0 / 0 | 138.5 / 0 / 0 (+3.1%) | 147.7 / 0 / 0 (+10.0%) | 135.6 / 0 / 0 (+1.0%) | 177.1 / 64 / 1 (+31.9%) |
| Set | 1 | 8 | GCTime | 230.5 / 0 / 0 | 218.7 / 0 / 0 (-5.1%) | 231.7 / 0 / 0 (+0.5%) | 245.9 / 0 / 0 (+6.7%) | 238.8 / 64 / 1 (+3.6%) |

### Snapshots on optimized GCTime

Both candidates compare with `febe4c7`. Their fast path targets idle entries with only GCTime enabled. Default and MaxAge rows measure the ordinary paths on those builds.

| Operation | Keys | Workers | Policy | **Optimized GCTime** | **First Get after a write** | **On every write** |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Get | 1,024 | 1 | Default | 31.24 / 0 / 0 | 31.34 / 0 / 0 (+0.3%) | 31.06 / 0 / 0 (-0.6%) |
| Get | 1,024 | 8 | Default | 12.47 / 0 / 0 | 13.5 / 0 / 0 (+8.3%) | 12.49 / 0 / 0 (+0.2%) |
| Get | 1,024 | 1 | MaxAge | 68.42 / 0 / 0 | 64.72 / 0 / 0 (-5.4%) | 69.76 / 0 / 0 (+2.0%) |
| Get | 1,024 | 8 | MaxAge | 20.94 / 0 / 0 | 18.87 / 0 / 0 (-9.9%) | 21.71 / 0 / 0 (+3.7%) |
| Get | 1,024 | 1 | GCTime | 48.73 / 0 / 0 | 44.39 / 0 / 0 (-8.9%) | 44.51 / 0 / 0 (-8.7%) |
| Get | 1,024 | 8 | GCTime | 17.55 / 0 / 0 | 8.78 / 0 / 0 (-50.0%) | 8.533 / 0 / 0 (-51.4%) |
| Get | 1 | 1 | Default | 25.73 / 0 / 0 | 26.36 / 0 / 0 (+2.4%) | 27.11 / 0 / 0 (+5.4%) |
| Get | 1 | 8 | Default | 102.9 / 0 / 0 | 127.3 / 0 / 0 (+23.7%) | 105.4 / 0 / 0 (+2.4%) |
| Get | 1 | 1 | MaxAge | 56.2 / 0 / 0 | 56.72 / 0 / 0 (+0.9%) | 56.62 / 0 / 0 (+0.7%) |
| Get | 1 | 8 | MaxAge | 117.2 / 0 / 0 | 118.4 / 0 / 0 (+1.0%) | 112.1 / 0 / 0 (-4.4%) |
| Get | 1 | 1 | GCTime | 42.23 / 0 / 0 | 38.21 / 0 / 0 (-9.5%) | 37.06 / 0 / 0 (-12.2%) |
| Get | 1 | 8 | GCTime | 128.3 / 0 / 0 | 81.54 / 0 / 0 (-36.4%) | 82.51 / 0 / 0 (-35.7%) |
| Set | 1,024 | 1 | Default | 83.24 / 0 / 0 | 83.8 / 0 / 0 (+0.7%) | 88.26 / 0 / 0 (+6.0%) |
| Set | 1,024 | 8 | Default | 36.44 / 0 / 0 | 38.44 / 0 / 0 (+5.5%) | 36.51 / 0 / 0 (+0.2%) |
| Set | 1,024 | 1 | MaxAge | 87.33 / 0 / 0 | 94.33 / 0 / 0 (+8.0%) | 93.05 / 0 / 0 (+6.5%) |
| Set | 1,024 | 8 | MaxAge | 35.31 / 0 / 0 | 40.16 / 0 / 0 (+13.7%) | 34.47 / 0 / 0 (-2.4%) |
| Set | 1,024 | 1 | GCTime | 135.4 / 0 / 0 | 119.3 / 0 / 0 (-11.9%) | 154.8 / 32 / 1 (+14.3%) |
| Set | 1,024 | 8 | GCTime | 44.26 / 0 / 0 | 65.41 / 0 / 0 (+47.8%) | 51.24 / 32 / 1 (+15.8%) |
| Set | 1 | 1 | Default | 75.01 / 0 / 0 | 75.6 / 0 / 0 (+0.8%) | 76.68 / 0 / 0 (+2.2%) |
| Set | 1 | 8 | Default | 221.8 / 0 / 0 | 221.2 / 0 / 0 (-0.3%) | 215.1 / 0 / 0 (-3.0%) |
| Set | 1 | 1 | MaxAge | 78.57 / 0 / 0 | 87.53 / 0 / 0 (+11.4%) | 81.44 / 0 / 0 (+3.7%) |
| Set | 1 | 8 | MaxAge | 224.1 / 0 / 0 | 226.2 / 0 / 0 (+0.9%) | 220.3 / 0 / 0 (-1.7%) |
| Set | 1 | 1 | GCTime | 105.8 / 0 / 0 | 110 / 0 / 0 (+4.0%) | 126.8 / 32 / 1 (+19.8%) |
| Set | 1 | 8 | GCTime | 233.7 / 0 / 0 | 245.9 / 0 / 0 (+5.2%) | 242.8 / 32 / 1 (+3.9%) |

## Original measurements

The three untyped read/write studies below used eight 300 ms samples per build. Each warm read cycles over 1,024 keys. Continuous overwrites and alternating pairs use one key. A pair counts **one Set followed by one untyped Get** as one operation. With eight workers, pairs can interleave. Their fractional mallocs/pair counter captures allocations that Go's integer allocs/op rounds down.

### First read after a write

The `2199cfe` → `018eadd` comparison explains the retirement decision. Eight-worker hot reads took about half as long, and continuous overwrites kept zero allocations. The first read after each write built another snapshot, raising the single-worker pair cost by 20.2%.

| Operation | Workers | Baseline ns/op | Candidate ns/op | Time change | B/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 48.095 | 42.605 | −11.4% | 0 → 0 | 0 → 0 |
| Warm Get, 1,024 keys | 8 | 17.415 | 8.467 | −51.4% | 0 → 0 | 0 → 0 |
| Same-key overwrite | 1 | 104.10 | 105.45 | +1.3% | 0 → 0 | 0 → 0 |
| Same-key overwrite | 8 | 248.15 | 246.25 | −0.8% | 0 → 0 | 0 → 0 |
| Same-key Set + Get pair | 1 | 152.15 | 182.95 | +20.2% | 0 → 32 | 0 → 1 |
| Same-key Set + Get pair | 8 | 254.65 | 285.65 | +12.2% | 0 → 31 | ~0 → 0.9903 |

[Original method and raw samples](benchmarks/cache_comparison/LAZY_GET_RESULTS.md)


### Original write-time snapshots

The four-build comparison includes the optimized baseline, first-read snapshot, original indexed-task baseline and original write-time snapshot respectively. Times are ns/op. The second table gives B/op and allocations/op.

| Workload | Workers | Current | First Get | Original control | Each Set | Each Set vs current | Each Set vs original |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 48.745 | 43.815 | 77.230 | 59.460 | +22.0% | -23.0% |
| Warm Get, 1,024 keys | 8 | 17.765 | 8.547 | 22.740 | 11.525 | -35.1% | -49.3% |
| Overwrite, one key | 1 | 108.350 | 106.050 | 129.500 | 167.350 | +54.5% | +29.2% |
| Overwrite, one key | 8 | 253.750 | 247.650 | 258.200 | 237.050 | -6.6% | -8.2% |
| Set + Get pair, one key | 1 | 154.950 | 189.000 | 202.050 | 219.800 | +41.9% | +8.8% |
| Set + Get pair, one key | 8 | 257.700 | 285.550 | 320.950 | 317.050 | +23.0% | -1.2% |

| Workload | Workers | Current | First Get | Original control | Each Set |
| --- | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Warm Get, 1,024 keys | 8 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Overwrite, one key | 1 | 0 / 0 | 0 / 0 | 0 / 0 | 64 / 1 |
| Overwrite, one key | 8 | 0 / 0 | 0 / 0 | 0 / 0 | 64 / 1 |
| Set + Get pair, one key | 1 | 0 / 0 | 32 / 1 | 0 / 0 | 64 / 1 |
| Set + Get pair, one key | 8 | 0 / 0 | 31 / 0.9904 | 0 / 0 | 64 / 1 |

[Original method and raw samples](benchmarks/cache_comparison/EAGER_SNAPSHOT_RESULTS.md)


### Write-time snapshots on optimized GCTime

All three builds include the entry-local task and direct elapsed time. The two snapshot variants use the same 32-byte record.

| Workload | Workers | Baseline ns/op | First Get ns/op | Each Set ns/op | Each Set vs baseline | Each Set vs first Get |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 48.475 | 43.755 | 44.020 | -9.2% | +0.6% |
| Warm Get, 1,024 keys | 8 | 17.490 | 8.639 | 8.647 | -50.6% | +0.1% |
| Same-key overwrite | 1 | 104.950 | 107.000 | 132.900 | +26.6% | +24.2% |
| Same-key overwrite | 8 | 251.700 | 248.400 | 243.150 | -3.4% | -2.1% |
| Same-key Set + Get pair | 1 | 155.700 | 190.950 | 172.000 | +10.5% | -9.9% |
| Same-key Set + Get pair | 8 | 257.200 | 289.800 | 277.350 | +7.8% | -4.3% |

| Workload | Workers | Baseline | First Get | Each Set |
| --- | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 0 B / 0 | 0 B / 0 | 0 B / 0 |
| Warm Get, 1,024 keys | 8 | 0 B / 0 | 0 B / 0 | 0 B / 0 |
| Same-key overwrite | 1 | 0 B / 0 | 0 B / 0 | 32 B / 1 |
| Same-key overwrite | 8 | 0 B / 0 | 0 B / 0 | 32 B / 1 |
| Same-key Set + Get pair | 1 | 0 B / 0 | 32 B / 1 | 32 B / 1 |
| Same-key Set + Get pair | 8 | 0 B / 0 | 31 B / 0.9901 | 32 B / 1 |

[Original method and raw samples](benchmarks/cache_comparison/OPTIMIZED_GCTIME_WRITE_SNAPSHOT.md)

### Read costs isolated in diagnostic controls

| Control | Workload | Measured result |
| --- | --- | --- |
| Skip the timestamp update on `938547f` | Single-key typed Get, GCTime only, 1 / 8 workers | 65.325 / 125.40 → 64.94 / 117.70 ns/op. Both versions read the clock and task. Skipping the store breaks exact idle renewal. |
| Typed and untyped public Get on optimized GCTime | One key, 1 / 8 workers | Untyped 41.475 / 124.75 ns/op, typed 41.90 / 141.45. The eight-worker samples varied substantially. |
| Omit the bucket read lock on `febe4c7` | 8 workers, distinct keys, GCTime only | Eight keys forced into one bucket 108.15 → 10.27 ns/op. Eight separate buckets 6.70 → 6.50. For 1,024 naturally hashed keys, 16.54 → 7.51. Both builds used an inert timer callback and a read-only key set. This control cannot support concurrent Set safely. |

All three probes reported 0 B/op and 0 allocs/op. The bucket experiment distinguishes contention on a **bucket's** reader counter from contention on a key's idle timestamp. Different keys only share the reader counter when they land in the same bucket. The archive includes the control patches, harnesses, samples and CPU profile summaries. [Historical inputs](benchmarks/cache_comparison/history-experiment-inputs.tar.gz)

### Single-key studies with the original harnesses

When the two GCTime changes were restored in `febe4c7`, the original typed same-key comparison against the indexed-task implementation used six samples per cell. All cases measured 0 B/op and 0 allocs/op. The eight-worker sample ranges overlapped. [Original samples and harness in restore/](benchmarks/cache_comparison/history-experiment-inputs.tar.gz)

| Operation | Workers | Before ns/op | Restored ns/op | Time change |
| --- | ---: | ---: | ---: | ---: |
| Get | 1 | 66.08 | 42.90 | −35.1% |
| Get | 8 | 126.55 | 119.45 | −5.6% |
| Set | 1 | 129.75 | 106.85 | −17.6% |
| Set | 8 | 253.40 | 253.00 | −0.2% |

The separate typed GCTime confirmation run measured **66.37 → 48.65 → 42.02 ns/op** at one worker for the indexed task, direct elapsed time, and direct elapsed time plus a task on the entry. At eight workers the same sequence was **127.10 → 128.40 → 123.15**. In the reader-shard study the GCTime medians were **127.25 → 91.46 → 78.68 ns/op** at eight workers for one, two and four counters. [GCTime handoff](benchmarks/cache_comparison/GCTIME_HANDOFF.md)

The following tables preserve the historical benchmark names and each run's own baseline. A cell gives median ns/op / B/op / allocs/op. The original API-shape screen used freshness settings, including StaleTime. Atomic and reader-shard Get studies used typed reads. The published atomic confirmation compared `938547f` with `74c5b70`. The separate legacy reader-lane screen exercised the snapshot-returning API. [Atomic confirmation and samples](https://github.com/Waterkyuu/soroi/blob/5197cfb/benchmarks/cache_comparison/ATOMIC_VALUE_RESULTS.md) · [Archived harnesses and raw data](benchmarks/cache_comparison/history-experiment-inputs.tar.gz)

<details>
<summary>API shape, MaxAge, timer reuse, reader shards and atomic publication</summary>

| Run | Benchmark | 1 worker | 8 workers |
| --- | --- | ---: | ---: |
| API shape A | `SingleKeyValueRead/TypedGet` | 107.35 / 0 / 0 | 124.5 / 0 / 0 |
| API shape A | `SingleKeyValueRead/UntypedGet` | 95.775 / 0 / 0 | 123.8 / 0 / 0 |
| API shape A | `SingleKeyValueRead/Value` | 20.24 / 0 / 0 | 102.75 / 0 / 0 |
| API shape A | `SingleKeyValueRead/FreshValue` | 64.16 / 0 / 0 | 117.4 / 0 / 0 |
| API shape A | `SingleKeyValueRead/Otter` | 31.295 / 0 / 0 | 5.2515 / 0 / 0 |
| API shape B | `SingleKeyReadStale/Get` | 102.75 / 0 / 0 | 121.7 / 0 / 0 |
| API shape B | `SingleKeyReadStale/ValueOK` | 20.575 / 0 / 0 | 100.35 / 0 / 0 |
| API shape B | `SingleKeyReadStale/ValueOKStale` | 64.195 / 0 / 0 | 114.05 / 0 / 0 |
| API split: release/v0.4.0 | `HotGet` | 108.85 / 0 / 0 | 127.05 / 0 / 0 |
| API split: value-only branch | `HotGet` | 24.27 / 0 / 0 | 97.885 / 0 / 0 |
| MaxAge confirmation: before e8fa553 | `HotKeyHardExpiry/Soroi` | 86.775 / 0 / 0 | 231.85 / 0 / 0 |
| MaxAge confirmation: before e8fa553 | `HotKeyHardExpiry/Otter` | 30.945 / 0 / 0 | 5.2815 / 0 / 0 |
| MaxAge confirmation: before e8fa553 | `HotKeyPlainGet` | 23.53 / 0 / 0 | 100.415 / 0 / 0 |
| MaxAge confirmation: e8fa553 | `HotKeyHardExpiry/Soroi` | 57.25 / 0 / 0 | 113.3 / 0 / 0 |
| MaxAge confirmation: e8fa553 | `HotKeyHardExpiry/Otter` | 32.15 / 0 / 0 | 5.451 / 0 / 0 |
| MaxAge confirmation: e8fa553 | `HotKeyPlainGet` | 24.63 / 0 / 0 | 99.05 / 0 / 0 |
| GCTime: e8fa553 | `IdleHotKey/Soroi` | 300 / 208 / 4 | 385.25 / 208 / 4 |
| GCTime: e8fa553 | `IdleHotKey/Otter` | 44.47 / 0 / 0 | 7.66 / 0 / 0 |
| GCTime: 938547f | `IdleHotKey/Soroi` | 67.12 / 0 / 0 | 127.9 / 0 / 0 |
| GCTime: 938547f | `IdleHotKey/Otter` | 44.8 / 0 / 0 | 7.858 / 0 / 0 |
| Reader shards: one | `HotKey/Soroi/Pure` | 23.535 / 0 / 0 | 99.83 / 0 / 0 |
| Reader shards: one | `HotKey/Otter/Pure` | 18.735 / 0 / 0 | 3.141 / 0 / 0 |
| Reader shards: one | `HotKey/Soroi/MaxAge` | 58.32 / 0 / 0 | 118.55 / 0 / 0 |
| Reader shards: one | `HotKey/Otter/MaxAge` | 31.785 / 0 / 0 | 5.3325 / 0 / 0 |
| Reader shards: one | `HotKey/Soroi/GCTime` | 65.82 / 0 / 0 | 127.25 / 0 / 0 |
| Reader shards: one | `HotKey/Otter/GCTime` | 43.91 / 0 / 0 | 7.419 / 0 / 0 |
| Reader shards: two | `HotKey/Soroi/Pure` | 25.675 / 0 / 0 | 63.195 / 0 / 0 |
| Reader shards: two | `HotKey/Soroi/MaxAge` | 59.95 / 0 / 0 | 73.955 / 0 / 0 |
| Reader shards: two | `HotKey/Soroi/GCTime` | 67.185 / 0 / 0 | 91.46 / 0 / 0 |
| Reader shards: four | `HotKey/Soroi/Pure` | 25.6 / 0 / 0 | 34.575 / 0 / 0 |
| Reader shards: four | `HotKey/Soroi/MaxAge` | 60.465 / 0 / 0 | 39.08 / 0 / 0 |
| Reader shards: four | `HotKey/Soroi/GCTime` | 67.135 / 0 / 0 | 78.68 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Get/Pure` | 23.355 / 0 / 0 | 101.05 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Set/Pure` | 74.17 / 0 / 0 | 228.15 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Get/Pure` | 18.815 / 0 / 0 | 3.2495 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Set/Pure` | 76.525 / 24 / 1 | 192.6 / 24 / 1 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Get/MaxAge` | 58.635 / 0 / 0 | 114.05 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Set/MaxAge` | 80.84 / 0 / 0 | 228.1 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Get/MaxAge` | 31.875 / 0 / 0 | 5.367 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Set/MaxAge` | 89.06 / 0 / 0 | 227.7 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Get/GCTime` | 68.245 / 0 / 0 | 128.95 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Soroi/Set/GCTime` | 132.1 / 0 / 0 | 227.05 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Get/GCTime` | 43.89 / 0 / 0 | 7.4425 / 0 / 0 |
| Atomic publication: 938547f and Otter | `AtomicHotKey/Otter/Set/GCTime` | 89.085 / 0 / 0 | 229.2 / 0 / 0 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Get/Pure` | 19.635 / 0 / 0 | 4.8615 / 0 / 0 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Set/Pure` | 106.2 / 64 / 1 | 202.7 / 64 / 1 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Get/MaxAge` | 49.25 / 0 / 0 | 12.46 / 0 / 0 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Set/MaxAge` | 110.2 / 64 / 1 | 226.8 / 64 / 1 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Get/GCTime` | 47.345 / 0 / 0 | 83.305 / 0 / 0 |
| Atomic publication: prototype | `AtomicHotKey/Soroi/Set/GCTime` | 165.05 / 64 / 1 | 239 / 64 / 1 |
| Historical reader lanes with the snapshot Get: 1 | `SingleKeyLaneCount/Get` | 107.75 / 0 / 0 | 125.55 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 1 | `SingleKeyLaneCount/Set` | 79.985 / 8 / 1 | 232.05 / 8 / 1 |
| Historical reader lanes with the snapshot Get: 1 | `SingleKeyLaneCount/Read99Write1` | 107.15 / 0 / 0 | 142.95 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 1 | `SingleKeyLaneCount/Read15Write1` | 107.2 / 0 / 0 | 181.45 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 2 | `SingleKeyLaneCount/Get` | 112.5 / 0 / 0 | 82.435 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 2 | `SingleKeyLaneCount/Set` | 87.24 / 8 / 1 | 212.05 / 8 / 1 |
| Historical reader lanes with the snapshot Get: 2 | `SingleKeyLaneCount/Read99Write1` | 112.05 / 0 / 0 | 113.6 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 2 | `SingleKeyLaneCount/Read15Write1` | 111.9 / 0 / 0 | 172.8 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 4 | `SingleKeyLaneCount/Get` | 111.9 / 0 / 0 | 55.99 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 4 | `SingleKeyLaneCount/Set` | 93.66 / 8 / 1 | 174.25 / 8 / 1 |
| Historical reader lanes with the snapshot Get: 4 | `SingleKeyLaneCount/Read99Write1` | 111.75 / 0 / 0 | 98.255 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 4 | `SingleKeyLaneCount/Read15Write1` | 111.9 / 0 / 0 | 166.1 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 8 | `SingleKeyLaneCount/Get` | 111.65 / 0 / 0 | 42.86 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 8 | `SingleKeyLaneCount/Set` | 111.75 / 8 / 1 | 192.45 / 8 / 1 |
| Historical reader lanes with the snapshot Get: 8 | `SingleKeyLaneCount/Read99Write1` | 111.85 / 0 / 0 | 96.93 / 0 / 0 |
| Historical reader lanes with the snapshot Get: 8 | `SingleKeyLaneCount/Read15Write1` | 112.95 / 0 / 0 | 165.45 / 0 / 0 |

</details>

<details>
<summary>GC task placement and clock screens, including per-run Otter controls</summary>

| Run | Benchmark | 1 worker | 8 workers |
| --- | --- | ---: | ---: |
| reference | `AtomicHotKey/Soroi/Get/Pure` | 23.205 / 0 / 0 | 99.49 / 0 / 0 |
| reference | `AtomicHotKey/Soroi/Set/Pure` | 74.285 / 0 / 0 | 225 / 0 / 0 |
| reference | `AtomicHotKey/Otter/Get/Pure` | 18 / 0 / 0 | 3.1955 / 0 / 0 |
| reference | `AtomicHotKey/Otter/Set/Pure` | 78.73 / 24 / 1 | 190.7 / 24 / 1 |
| reference | `AtomicHotKey/Soroi/Get/GCTime` | 67.175 / 0 / 0 | 127 / 0 / 0 |
| reference | `AtomicHotKey/Soroi/Set/GCTime` | 130.5 / 0 / 0 | 248.75 / 0 / 0 |
| reference | `AtomicHotKey/Otter/Get/GCTime` | 43.64 / 0 / 0 | 7.682 / 0 / 0 |
| reference | `AtomicHotKey/Otter/Set/GCTime` | 89.635 / 0 / 0 | 229.4 / 0 / 0 |
| lookup | `AtomicHotKey/Soroi/Get/Pure` | 23.575 / 0 / 0 | 119.85 / 0 / 0 |
| lookup | `AtomicHotKey/Soroi/Set/Pure` | 77.14 / 0 / 0 | 228 / 0 / 0 |
| lookup | `AtomicHotKey/Soroi/Get/GCTime` | 56.76 / 0 / 0 | 128.9 / 0 / 0 |
| lookup | `AtomicHotKey/Soroi/Set/GCTime` | 119.5 / 0 / 0 | 262.65 / 0 / 0 |
| elapsed | `AtomicHotKey/Soroi/Get/Pure` | 23.97 / 0 / 0 | 101.5 / 0 / 0 |
| elapsed | `AtomicHotKey/Soroi/Set/Pure` | 75.285 / 0 / 0 | 227.2 / 0 / 0 |
| elapsed | `AtomicHotKey/Soroi/Get/GCTime` | 48.115 / 0 / 0 | 125.8 / 0 / 0 |
| elapsed | `AtomicHotKey/Soroi/Set/GCTime` | 116.25 / 0 / 0 | 247.45 / 0 / 0 |
| combined | `AtomicHotKey/Soroi/Get/Pure` | 24.505 / 0 / 0 | 102.25 / 0 / 0 |
| combined | `AtomicHotKey/Soroi/Set/Pure` | 74.35 / 0 / 0 | 226.6 / 0 / 0 |
| combined | `AtomicHotKey/Soroi/Get/GCTime` | 42.07 / 0 / 0 | 122.8 / 0 / 0 |
| combined | `AtomicHotKey/Soroi/Set/GCTime` | 105.95 / 0 / 0 | 244.35 / 0 / 0 |
| striped | `AtomicHotKey/Soroi/Get/Pure` | 24.535 / 0 / 0 | 99.735 / 0 / 0 |
| striped | `AtomicHotKey/Soroi/Set/Pure` | 75.11 / 0 / 0 | 224.25 / 0 / 0 |
| striped | `AtomicHotKey/Soroi/Get/GCTime` | 43.005 / 0 / 0 | 119.1 / 0 / 0 |
| striped | `AtomicHotKey/Soroi/Set/GCTime` | 116.05 / 0 / 0 | 251.95 / 0 / 0 |
| candidate | `AtomicHotKey/Soroi/Get/Pure` | 25.545 / 0 / 0 | 99.57 / 0 / 0 |
| candidate | `AtomicHotKey/Soroi/Set/Pure` | 77.75 / 0 / 0 | 223.7 / 0 / 0 |
| candidate | `AtomicHotKey/Soroi/Get/GCTime` | 42.255 / 0 / 0 | 122.8 / 0 / 0 |
| candidate | `AtomicHotKey/Soroi/Set/GCTime` | 106.75 / 0 / 0 | 247.6 / 0 / 0 |
| final-base | `AtomicHotKey/Soroi/Get/Pure` | 23.415 / 0 / 0 | 100.75 / 0 / 0 |
| final-base | `AtomicHotKey/Soroi/Set/Pure` | 76.295 / 0 / 0 | 223.8 / 0 / 0 |
| final-base | `AtomicHotKey/Otter/Get/Pure` | 18.305 / 0 / 0 | 3.1125 / 0 / 0 |
| final-base | `AtomicHotKey/Otter/Set/Pure` | 78.785 / 24 / 1 | 190.25 / 24 / 1 |
| final-base | `AtomicHotKey/Soroi/Get/GCTime` | 66.37 / 0 / 0 | 127.1 / 0 / 0 |
| final-base | `AtomicHotKey/Soroi/Set/GCTime` | 131.1 / 0 / 0 | 252.5 / 0 / 0 |
| final-base | `AtomicHotKey/Otter/Get/GCTime` | 43.97 / 0 / 0 | 7.5305 / 0 / 0 |
| final-base | `AtomicHotKey/Otter/Set/GCTime` | 88.96 / 0 / 0 | 229.05 / 0 / 0 |
| final-elapsed | `AtomicHotKey/Soroi/Get/Pure` | 24.03 / 0 / 0 | 102.85 / 0 / 0 |
| final-elapsed | `AtomicHotKey/Soroi/Set/Pure` | 74.13 / 0 / 0 | 227.1 / 0 / 0 |
| final-elapsed | `AtomicHotKey/Soroi/Get/GCTime` | 48.65 / 0 / 0 | 128.4 / 0 / 0 |
| final-elapsed | `AtomicHotKey/Soroi/Set/GCTime` | 116.45 / 0 / 0 | 243.6 / 0 / 0 |
| final-tail | `AtomicHotKey/Soroi/Get/Pure` | 24.265 / 0 / 0 | 100.055 / 0 / 0 |
| final-tail | `AtomicHotKey/Soroi/Set/Pure` | 75.46 / 0 / 0 | 225.75 / 0 / 0 |
| final-tail | `AtomicHotKey/Soroi/Get/GCTime` | 42.025 / 0 / 0 | 123.15 / 0 / 0 |
| final-tail | `AtomicHotKey/Soroi/Set/GCTime` | 105.25 / 0 / 0 | 256.05 / 0 / 0 |

</details>

The task-and-clock table uses original sample file labels. In the final confirmation, `final-base` uses the indexed task, `final-elapsed` uses direct elapsed time, and `final-tail` also stores the task at the end of the entry.

<details>
<summary>Atomic confirmation on the original indexed-task baseline</summary>

| Run | Benchmark | 1 worker | 8 workers |
| --- | --- | ---: | ---: |
| 938547f and Otter | `AtomicHotKey/Soroi/Get/Pure` | 23.37 / 0 / 0 | 103.6 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Soroi/Set/Pure` | 73.95 / 0 / 0 | 228.45 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Get/Pure` | 18.96 / 0 / 0 | 3.188 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Set/Pure` | 76.875 / 24 / 1 | 192.25 / 24 / 1 |
| 938547f and Otter | `AtomicHotKey/Soroi/Get/MaxAge` | 57.875 / 0 / 0 | 114.45 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Soroi/Set/MaxAge` | 78.585 / 0 / 0 | 232.25 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Get/MaxAge` | 31.86 / 0 / 0 | 5.399 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Set/MaxAge` | 89.635 / 0 / 0 | 229.3 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Soroi/Get/GCTime` | 65.73 / 0 / 0 | 128.8 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Soroi/Set/GCTime` | 129.8 / 0 / 0 | 243.95 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Get/GCTime` | 44.23 / 0 / 0 | 7.479 / 0 / 0 |
| 938547f and Otter | `AtomicHotKey/Otter/Set/GCTime` | 89.565 / 0 / 0 | 228.5 / 0 / 0 |
| 74c5b70 | `AtomicHotKey/Soroi/Get/Pure` | 19.51 / 0 / 0 | 4.656 / 0 / 0 |
| 74c5b70 | `AtomicHotKey/Soroi/Set/Pure` | 107.05 / 64 / 1 | 241 / 64 / 1 |
| 74c5b70 | `AtomicHotKey/Soroi/Get/MaxAge` | 49.47 / 0 / 0 | 10.605 / 0 / 0 |
| 74c5b70 | `AtomicHotKey/Soroi/Set/MaxAge` | 110.15 / 64 / 1 | 244.4 / 64 / 1 |
| 74c5b70 | `AtomicHotKey/Soroi/Get/GCTime` | 48.085 / 0 / 0 | 80.87 / 0 / 0 |
| 74c5b70 | `AtomicHotKey/Soroi/Set/GCTime` | 164.05 / 64 / 1 | 237.15 / 64 / 1 |

</details>

### Diagnostic subscriber indexing

A separate backfill measured the historical Fetch harness from `5cb7616:events_benchmark_test.go` on its parent `e4f8598` and on `5cb7616`. It times one cached Fetch with 0, 1, 100 or 1,000 unrelated exact-key subscriptions. The new index routes ordinary events to matching subscriptions. Both builds ran sequentially at CPU=1 with three alternating 100 ms samples per case on the matrix host. Results are ns/op / B/op / allocs/op. [Raw samples in events/samples.txt](benchmarks/cache_comparison/history-experiment-inputs.tar.gz)

| Unrelated subscriptions | Before | With key index | Time change |
| ---: | ---: | ---: | ---: |
| 0 | 128 / 1 / 1 | 127 / 1 / 1 | -0.8% |
| 1 | 196.9 / 1 / 1 | 184.2 / 1 / 1 | -6.4% |
| 100 | 684 / 1 / 1 | 183.7 / 1 / 1 | -73.1% |
| 1000 | 6636 / 1 / 1 | 180.2 / 1 / 1 | -97.3% |

## Reproduction

Run the current implementation from the repository root.

```sh
go test . -run '^$' -bench '^BenchmarkCacheBaseline$' -cpu=1,8 -benchtime=100ms -count=3 -benchmem
```

For historical comparisons, build the binaries first. The six named experimental patches in the input archive each apply independently to `938547f`. The commands below reproduce the source order and round boundaries in the raw sample file.

```sh
set -eu
scratch=$(mktemp -d)
harness="$PWD/cache_baseline_benchmark_test.go"
tar -xzf benchmarks/cache_comparison/history-experiment-inputs.tar.gz -C "$scratch"
forward='0250a1f e8fa553 938547f febe4c7 935be04 74c5b70 018eadd 5604a00 elapsed lookup combined striped two four'
reverse='four two striped combined lookup elapsed 5604a00 018eadd 74c5b70 935be04 febe4c7 938547f e8fa553 0250a1f'
for name in $forward; do
  rev=$name
  patch="$scratch/variants/$name.patch"
  if test -f "$patch"; then rev=938547f; fi
  dest="$scratch/$name"
  mkdir "$dest"
  git archive -o "$scratch/source.tar" "$rev"
  tar -xf "$scratch/source.tar" -C "$dest"
  if test -f "$patch"; then (cd "$dest" && git apply --no-index "$patch"); fi
  cp "$harness" "$dest/cache_baseline_benchmark_test.go"
  go -C "$dest" test -mod=readonly -c -o "$scratch/$name.test" cache_baseline_benchmark_test.go
done
round=0
for order in "$forward" "$reverse" "$forward"; do
  round=$((round + 1))
  for name in $order; do
    echo "# round=$round source=$name"
    "$scratch/$name.test" '-test.run=^$' '-test.bench=^BenchmarkCacheBaseline$' \
      -test.cpu=1,8 -test.benchtime=100ms -test.count=1 -test.benchmem
  done
done > "$scratch/samples.txt"
```

Use a POSIX shell for the reproduction block. Each source must produce 24 cases in each of the three rounds. The benchmark setup, including the first read of each populated key, is outside the timer.
