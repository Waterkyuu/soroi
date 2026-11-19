# Write-time snapshots on optimized GCTime

The baseline is `a5a5f47`, with its GC task stored on the cache entry and direct
monotonic elapsed time for default-clock renewals. The new candidate is `5604a00`.
The first-read comparison is `018eadd`, which also includes both GCTime optimizations.
All three use the same untyped Get benchmark source from `e24452d`.

The new candidate publishes the immutable value and static type into the idle task's
atomic pointer while holding the bucket write lock. The snapshot is the same size and
has the same fields as the first-read version. With this candidate a worker can read
the latest version after Set without building a snapshot or taking a bucket read lock.
Reads still renew the exact idle deadline and revalidate the published version. The
experimental path covers idle keys using default time, positive GCTime and disabled
MaxAge and capacity limits.

## Results

| Workload | Workers | Baseline ns/op | First Get ns/op | Each Set ns/op | Each Set vs baseline | Each Set vs first Get |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 48.475 | 43.755 | 44.020 | -9.2% | +0.6% |
| Warm Get, 1,024 keys | 8 | 17.490 | 8.639 | 8.647 | -50.6% | +0.1% |
| Same-key overwrite | 1 | 104.950 | 107.000 | 132.900 | +26.6% | +24.2% |
| Same-key overwrite | 8 | 251.700 | 248.400 | 243.150 | -3.4% | -2.1% |
| Same-key Set + Get pair | 1 | 155.700 | 190.950 | 172.000 | +10.5% | -9.9% |
| Same-key Set + Get pair | 8 | 257.200 | 289.800 | 277.350 | +7.8% | -4.3% |

Memory cells show bytes and allocations per operation. One mixed operation is a
complete Set followed by one Get. The write workloads report fractional mallocs/op
rounded to four decimal places. This includes amortized harness overhead. The raw
output also includes Go's truncated integer counters.

| Workload | Workers | Baseline | First Get | Each Set |
| --- | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 0 B / 0 | 0 B / 0 | 0 B / 0 |
| Warm Get, 1,024 keys | 8 | 0 B / 0 | 0 B / 0 | 0 B / 0 |
| Same-key overwrite | 1 | 0 B / 0 | 0 B / 0 | 32 B / 1 |
| Same-key overwrite | 8 | 0 B / 0 | 0 B / 0 | 32 B / 1 |
| Same-key Set + Get pair | 1 | 0 B / 0 | 32 B / 1 | 32 B / 1 |
| Same-key Set + Get pair | 8 | 0 B / 0 | 31 B / 0.9901 | 32 B / 1 |

## Interpretation

The two publication strategies use identical hot-reader code. At eight workers their
warm-read medians were 8.639 and 8.647 ns/op and the sample ranges overlapped.
Publishing on Set paid one 32-byte allocation on every overwrite, including workloads
with no intervening reads. Eight-worker overwrite times overlapped the baseline range.
Publishing on Set lowered alternating-pair time relative to first-read publication
by 9.9% at one worker and 4.3% at eight workers. Both mixed cases remained slower
than the baseline, which has zero amortized allocations in these workloads.

## Method

Apple M3 Pro, Darwin/arm64, Go 1.27.1, GCTime one hour, real timers and the default
clock. Warm reads cycle through disjoint stripes of 1,024 naturally hashed string
keys using `client.Get(key)`. Set and alternating pairs share one key and alternate
between the small integer values 42 and 43. Concurrent Set/Get pairs can interleave.
Warm-up and initial publication are outside the timed section. Parallel ns/op is
elapsed time divided by all completed operations. Each cell is a median of eight
samples. Binaries ran sequentially in different orders in each round.

Run from the repository root in a POSIX shell.

```sh
scratch=$(mktemp -d)
git show e24452d:lazy_read_benchmark_test.go > "$scratch/lazy_read_benchmark_test.go"
for label in baseline first-get each-set; do
  case "$label" in
    baseline) rev=a5a5f47 ;;
    first-get) rev=018eadd ;;
    each-set) rev=5604a00 ;;
  esac
  mkdir "$scratch/$label"
  git archive -o "$scratch/$label.tar" "$rev"
  tar -xf "$scratch/$label.tar" -C "$scratch/$label"
  go -C "$scratch/$label" test -mod=readonly -c -o "$scratch/$label.test" "$scratch/lazy_read_benchmark_test.go"
done
for order in 'baseline first-get each-set' 'baseline each-set first-get' \
  'first-get baseline each-set' 'first-get each-set baseline' \
  'each-set baseline first-get' 'each-set first-get baseline' \
  'baseline first-get each-set' 'each-set first-get baseline'; do
  for label in $order; do
    "$scratch/$label.test" -test.run='^$' -test.bench='^BenchmarkLazyGet$' \
      -test.cpu=1,8 -test.benchmem -test.benchtime=300ms >> "$scratch/$label.txt"
  done
done
```

[All 144 measurements](optimized-gctime-write-snapshot-darwin-arm64.txt) · [Historical prototype comparison](EAGER_SNAPSHOT_RESULTS.md)
