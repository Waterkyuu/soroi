# Atomic single-key value benchmark

The experimental branch `waterkyuu/perf/atomic-key-publication` publishes an
immutable value, static type, and installation time through one atomic pointer
per key. Eligible `Get` hits skip the bucket's reader registration. The
existing bucket locks still protect writes, full query state, and cache
lifecycle. With `GCTime` enabled, hits record exact access times and the
collector atomically claims the idle task before removing the key.

## Results

The baseline is `938547fea801be019641e9f007ae95ec164f49ad`. The measured
candidate is `74c5b709e861aabcef3a39eca302a3c6d83dbce7`. Measurements use
Apple M3 Pro, Darwin ARM64, Go 1.27.1, and Otter v2.4.0.

Each cell shows **one worker / eight workers**, in ns/op, using the median of
six samples per case. At eight workers, ns/op is the wall time divided by total
completed operations across all workers. Individual latency and tail latency
need separate measurements. [Go parallel benchmark contract](https://pkg.go.dev/testing#B.RunParallel)

| Operation | Soroi baseline | Soroi atomic | Otter v2.4.0 |
| --- | ---: | ---: | ---: |
| `Get`, pure | 23.37 / 103.60 | 19.51 / 4.66 | 18.96 / 3.19 |
| `Get`, only `MaxAge` | 57.88 / 114.45 | 49.47 / 10.61 | 31.86 / 5.40 |
| `Get`, only `GCTime` | 65.73 / 128.80 | 48.09 / 80.87 | 44.23 / 7.48 |
| `Set`, pure | 73.95 / 228.45 | 107.05 / 241.00 | 76.88 / 192.25 |
| `Set`, only `MaxAge` | 78.59 / 232.25 | 110.15 / 244.40 | 89.64 / 229.30 |
| `Set`, only `GCTime` | 129.80 / 243.95 | 164.05 / 237.15 | 89.57 / 228.50 |

Eight-worker pure `Get` fell from 103.60 to 4.66 ns/op. With precise
idle renewal, it fell from 128.80 to 80.87 ns/op, against Otter's 7.48.
That case still writes one shared access timestamp. Its cost was not
measured independently of the other idle-path work.

Both Soroi versions used zero per-hit allocations. The candidate adds
one 64 B immutable result allocation per overwrite. At one worker,
`Set` median cost increased by 26% to 45%. At eight workers the median
changes ranged from 3% lower to 5.5% higher. The reported changes are
ratios of the medians in this run.

Reads are hits on a single preloaded string key with a `uint64` value.
Writes repeatedly overwrite the same key, alternating between `42` and
`43` per worker. Read and write cases run in separate timed intervals.
Each client uses its default clock, with capacity limits, subscriptions,
and source loaders absent. The pure case has no retention policy. Soroi
`MaxAge` is compared with Otter `ExpiryWriting`, while Soroi `GCTime` is
compared with Otter `ExpiryAccessing`. Each enabled policy uses one hour.
Otter's access renewal can skip small changes to the deadline. The
candidate records each eligible hit's access time precisely.
[Otter renewal rule](https://github.com/maypok86/otter/blob/v2.4.0/deadline.go#L28)

## Reproduce

Use the same Go version and run from the repository root on an otherwise idle
machine. Export both measured revisions and compile them with the same
benchmark harness. Run the binaries in sequence, separately from builds,
profilers, and race tests. [Benchmark source](hot_key_benchmark_test.go)

```sh
SOROI_ATOMIC_DIR=$(mktemp -d)
SOROI_ATOMIC_HARNESS="$PWD/benchmarks/cache_comparison/hot_key_benchmark_test.go"

for variant in baseline candidate; do
  revision=938547fea801be019641e9f007ae95ec164f49ad
  if [ "$variant" = candidate ]; then
    revision=74c5b709e861aabcef3a39eca302a3c6d83dbce7
  fi
  mkdir "$SOROI_ATOMIC_DIR/$variant"
  git archive -o "$SOROI_ATOMIC_DIR/$variant.tar" "$revision"
  tar -xf "$SOROI_ATOMIC_DIR/$variant.tar" -C "$SOROI_ATOMIC_DIR/$variant"
  go -C "$SOROI_ATOMIC_DIR/$variant/benchmarks/cache_comparison" test -mod=readonly -c \
    -o "$SOROI_ATOMIC_DIR/$variant.test" "$SOROI_ATOMIC_HARNESS"
done

for variant in baseline candidate candidate baseline; do
  pattern='^BenchmarkAtomicHotKey/Soroi$'
  if [ "$variant" = baseline ]; then
    pattern='^BenchmarkAtomicHotKey$'
  fi
  "$SOROI_ATOMIC_DIR/$variant.test" -test.run '^$' -test.bench "$pattern" \
    -test.benchmem -test.benchtime=200ms -test.count=3 -test.cpu=1,8 \
    >> "$SOROI_ATOMIC_DIR/$variant.txt"
done
```

The baseline output includes Soroi and Otter. The candidate output includes the
new Soroi path. Verify each case has six samples for each worker count and
compare medians. [Baseline and Otter samples](atomic-value-baseline-darwin-arm64.txt) ·
[Candidate samples](atomic-value-candidate-darwin-arm64.txt)

The candidate passed the full race suite and Go 1.22.12 tests. Focused tests
exercise the bucket lock bypass, age revalidation after a replacement,
renewal racing with collection, and an old idle task racing with removal
and recreation. These measurements cover warm same-key reads and overwrites
on this host. They leave mixed workloads, active subscriptions, bounded
capacity, and Linux runtime performance for separate measurement.
