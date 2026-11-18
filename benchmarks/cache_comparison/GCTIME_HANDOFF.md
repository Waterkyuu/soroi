# GCTime performance handoff

The measured source baseline for `waterkyuu/feat/typed-cache-value` is
`938547f`. It records the exact last access to an idle key and reuses the
existing cleanup timer. Each default-clock value hit still takes a bucket
read lock and updates its per-key last-access timestamp.

## Baseline and benchmark scope

The table shows typed `Get[uint64]` hits on the same preloaded string key.
Soroi uses only `GCTime: time.Hour` and Otter v2.4.0 uses
`ExpiryAccessing(1h)`. Both use the default clock with capacity limits,
active handles, and loaders absent. Neither Soroi `MaxAge` nor a freshness
window is configured. Results are medians of six samples on Apple M3 Pro,
Darwin ARM64, Go 1.27.1.

| Implementation | One worker (ns/op) | Eight workers (ns/op) |
| --- | ---: | ---: |
| Soroi current source, `938547f` | 66.37 | 127.10 |
| Otter `GetIfPresent` | 43.97 | 7.53 |

These are from the same confirmation run. At eight workers, ns/op represents
wall time divided by the total operations completed by all workers.
[Go parallel benchmark contract](https://pkg.go.dev/testing#B.RunParallel)

Otter's hash lookup uses atomic reads and skips bucket reader registration.
Its access-expiry path can also skip storing a deadline change of up to one
second while both lifetimes exceed that tolerance. Soroi preserves the full
idle window measured from the latest exact access. Both differences matter
when many workers read one key.
[Otter hash lookup](https://github.com/maypok86/otter/blob/v2.4.0/internal/hashmap/map.go#L203) ·
[Otter expiry tolerance](https://github.com/maypok86/otter/blob/v2.4.0/deadline.go#L28)

## Optimization in the current branch

Commit `938547f` replaced timer stop-and-recreate on each default-clock
read with an atomic update to the existing task.

1. The bucket read lock protects the entry and its timer's identity. The
   read finds the task in `gcTasks`, samples the clock, and calculates the
   offset from that task's start time.
2. Atomic compare-and-swap retains the largest offset. A read with an
   earlier timestamp leaves a later recorded value intact.
3. When the timer fires, its callback takes the bucket write lock, checks
   that the task still belongs to this key, and computes the deadline from
   the latest offset. It rearms for the remaining duration or removes the
   idle key. Active handles or loads suspend cleanup. Injected clocks retain
   the serialized timer-reset behavior.

In that experiment, Soroi went from **300.00 / 385.25** to
**67.12 / 127.90 ns/op** at one and eight workers. Otter measured
**44.62 / 7.66 ns/op**. Soroi's per-hit allocation went from **208 B and
four allocations** to zero. The tests cover out-of-order concurrent renewals,
the last nanosecond before expiration, and a stopped callback after the key
has been recreated.
[Current implementation](../../gc.go) · [Public value read](../../query.go) ·
[Exact renewal tests](../../gc_read_test.go)

## Experiments to compare against

Every row below is the same one-key, `GCTime`-only read workload. Compare
medians within a run. Otter rows come from the runs that measured Otter.

| Run | Variant | One worker (ns/op) | Eight workers (ns/op) |
| --- | --- | ---: | ---: |
| Task lookup and clock confirmation | Current source | 66.37 | 127.10 |
| Task lookup and clock confirmation | Direct monotonic elapsed time only | 48.65 | 128.40 |
| Task lookup and clock confirmation | Direct elapsed time and task on the entry | 42.02 | 123.15 |
| Task lookup and clock confirmation | Otter | 43.97 | 7.53 |
| Exact timestamp sharding screen | Direct elapsed time and task on the entry | 42.07 | 122.80 |
| Exact timestamp sharding screen | Same candidate with four timestamp slots | 43.01 | 119.10 |
| Reader registration sharding | Original bucket counter | 65.82 | 127.25 |
| Reader registration sharding | Two padded counters per bucket | 67.19 | 91.46 |
| Reader registration sharding | Four padded counters per bucket | 67.14 | 78.68 |
| Reader registration sharding | Otter | 43.91 | 7.42 |
| Atomic value publication | Current source | 65.73 | 128.80 |
| Atomic value publication | Atomic value and exact idle renewal | 48.09 | 80.87 |
| Atomic value publication | Otter | 44.23 | 7.48 |

- **Task on the entry and direct elapsed time.** Move the GC task from a
  separate map onto the cache entry, so a read obtains it from the entry
  it already found. On the default clock, `time.Since` gets elapsed
  monotonic time directly. The combined candidate reduced one-worker
  read cost by about 37%, with overlapping eight-worker samples. In the
  same run, Soroi's `Set` with only `GCTime` moved from
  131.10 / 252.50 to 105.25 / 256.05 ns/op. These changes were tested in
  `9b3f0d3` and subsequently removed from the current branch.
- **Four exact timestamp slots.** Choose a slot from the monotonic offset's
  bits. The callback takes the maximum across slots under the bucket write
  lock. The eight-worker improvement in this screen was small, while padded
  slots increased memory per task. The implementation stayed in the experiment.
- **Reader registration.** Split each bucket's reader registration across
  padded locks selected by the Go scheduler processor. This improved
  eight-worker hits while keeping exact renewals. The prototype depended on
  private runtime functions and required a writer to acquire every shard.
- **Atomic value publication.** Publish an immutable result for the key and
  bypass bucket reader registration on eligible hits. Each idle hit still
  records the exact timestamp. Collection uses a compare-and-swap retirement
  step to detect a concurrent renewal. The one-worker `Set` with only
  `GCTime` went from 129.80 to 164.05 ns/op, with one additional 64 B
  allocation per overwrite. This prototype and its raw samples are on the
  separate branch `waterkyuu/perf/atomic-key-publication`.
  [Atomic benchmark report](https://github.com/Waterkyuu/soroi/blob/5197cfbaf8d34773a215e7d13591be2127ac5ce5/benchmarks/cache_comparison/ATOMIC_VALUE_RESULTS.md)

## Reproduce the current baseline

Run from the repository root. The harness is tracked at `5197cfb` on the
separate atomic-publication branch. Export the measured source and copy the
harness to a temporary directory. Use Go 1.27.1 for comparison with the
recorded environment.

```sh
SOROI_GC_DIR=$(mktemp -d)
mkdir "$SOROI_GC_DIR/source"
git archive -o "$SOROI_GC_DIR/source.tar" 938547f
tar -xf "$SOROI_GC_DIR/source.tar" -C "$SOROI_GC_DIR/source"
git show 5197cfb:benchmarks/cache_comparison/hot_key_benchmark_test.go \
  > "$SOROI_GC_DIR/hot_key_benchmark_test.go"
go -C "$SOROI_GC_DIR/source/benchmarks/cache_comparison" test -mod=readonly \
  -run '^$' -bench '^BenchmarkAtomicHotKey$/^(Soroi|Otter)$/^Get$/^GCTime$' \
  -benchmem -benchtime=200ms -count=6 -cpu=1,8 \
  "$SOROI_GC_DIR/hot_key_benchmark_test.go"
```

Expect `Soroi/Get/GCTime` and `Otter/Get/GCTime` in the output. A suffix of
`-8` marks the eight-worker case. Compare the six samples per case by median.

## Next investigation

Keep the bucket reader architecture fixed for the first round. In a
temporary copy, isolate the timestamp store with a diagnostic variant that
samples the clock and reads the task but skips the compare-and-swap. That
variant is only a measurement control. Use the result to separate timestamp
contention from reader registration and lookup costs. Any shipping candidate
must retain precise access recording, pass the deadline and task-identity
tests, and report `Set` overhead. Apply production changes to the feature
branch only after explicit approval.
