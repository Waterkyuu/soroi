# First-read publication with GCTime

The experiment cuts warm multi-key read time by 51.4% at eight workers. Continuous
overwrites retain zero amortized allocations. A single worker alternating writes
and reads of one key pays for one new 32-byte snapshot per pair. At eight workers
this workload also slows down, by 12.2%.

The implementation was retired in `22c2300`. Git retains the measured candidate
in `018eadd` and its benchmark harness in `e24452d`.

## Measurements

Baseline 2199cfe and candidate 018eadd ran on Apple M3 Pro, Darwin/arm64, Go 1.27.1. Each cell shows the median of eight
samples per version with 300 ms per case and GOMAXPROCS set to 1 or 8.
Baseline and candidate binaries ran sequentially in alternating order. Both
used the same benchmark source, default clock, real timers and `GCTime: time.Hour`.

| Operation | Workers | Baseline ns/op | Candidate ns/op | Time change | B/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Warm Get, 1,024 keys | 1 | 48.095 | 42.605 | −11.4% | 0 → 0 | 0 → 0 |
| Warm Get, 1,024 keys | 8 | 17.415 | 8.467 | −51.4% | 0 → 0 | 0 → 0 |
| Same-key overwrite | 1 | 104.10 | 105.45 | +1.3% | 0 → 0 | 0 → 0 |
| Same-key overwrite | 8 | 248.15 | 246.25 | −0.8% | 0 → 0 | 0 → 0 |
| Same-key Set + Get pair | 1 | 152.15 | 182.95 | +20.2% | 0 → 32 | 0 → 1 |
| Same-key Set + Get pair | 8 | 254.65 | 285.65 | +12.2% | 0 → 31 | ~0 → 0.9903 |

Go truncates its standard B/op and allocs/op counters. The mixed benchmark
also reports the process malloc delta divided by completed pairs, including
amortized harness overhead. At eight workers the candidate's standard counter
prints 0 allocs/op while the fractional median is 0.9903 mallocs/pair.
The before/after time ranges overlap for both overwrite cases.

- **Warm reads.** All 1,024 string keys were set and read before timing. Each
  worker repeatedly calls the untyped `client.Get(key)` over its own disjoint
  stripe of keys. Default hashing selects buckets. All hits validate the value.
- **Overwrites.** Workers repeatedly set the same prewarmed key, alternating
  small integers 42 and 43. No read occurs inside the timed loop.
- **Alternating.** One measured operation comprises one Set followed by one
  untyped Get by the same worker. At eight workers the pairs interleave, so
  multiple workers can reuse one published version. Parallel ns/op measures
  aggregate throughput. The benchmark does not measure individual call latency.

## Implementation scope

The fast path handles retained values with an idle GC task, the default clock,
positive GCTime and disabled MaxAge and MaxEntries. The first successful value
read publishes the value and its static type while holding the bucket read lock.
Subsequent readers find the task in a concurrent index, renew the exact idle
deadline and recheck the publication. Set invalidates the snapshot while updating
the existing entry. Retiring the task prevents a delayed reader from renewing a
removed key. Active loads, subscriptions and other policies use the locked path.

The warm-read sample excludes the first publication and its retained memory.
This tradeoff favors versions read repeatedly. Alternating workloads pay the
publication cost on the first Get after each write. Results are specific to
these key distributions, worker counts, value sizes and machine.

## Reproduction

Run from the repository root. Use Go 1.27.1 to match
the recorded samples. This builds independent binaries from the baseline and
the experiment with the same external benchmark file.

```sh
scratch=$(mktemp -d)
git show e24452d:lazy_read_benchmark_test.go > "$scratch/lazy_read_benchmark_test.go"
for variant in baseline candidate; do
  rev=2199cfe
  if [ "$variant" = candidate ]; then rev=018eadd; fi
  mkdir "$scratch/$variant"
  git archive -o "$scratch/$variant.tar" "$rev"
  tar -xf "$scratch/$variant.tar" -C "$scratch/$variant"
  go -C "$scratch/$variant" test -mod=readonly -c -o "$scratch/$variant.test" \
    "$scratch/lazy_read_benchmark_test.go"
done

for round in 1 2 3 4 5 6 7 8; do
  set -- baseline candidate
  if [ "$((round % 2))" -eq 0 ]; then set -- candidate baseline; fi
  for variant in "$@"; do
    "$scratch/$variant.test" -test.run='^$' -test.bench='^BenchmarkLazyGet$' \
      -test.benchmem -test.cpu=1,8 -test.benchtime=300ms >> "$scratch/$variant.txt"
  done
done
```

[All 96 measurements](lazy-get-darwin-arm64.txt)
