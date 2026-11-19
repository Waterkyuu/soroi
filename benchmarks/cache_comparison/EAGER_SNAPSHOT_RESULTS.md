# Eager single-key snapshots on the untyped Get benchmark

This run measures the archived implementation that allocates an immutable result on
every overwrite and publishes it through an atomic pointer. All four binaries used
the byte-identical benchmark source from `e24452d:lazy_read_benchmark_test.go`.

| Build | Revision | Read and write behavior |
| --- | --- | --- |
| Current | `bd7714d` | Bucket read lock, GC task on the entry, direct monotonic elapsed time. |
| First Get | `018eadd` | The first eligible read publishes the value. Overwriting it invalidates that publication. |
| Original control | `938547f` | The baseline used for the original eager prototype, with an indexed GC task and a clock sample followed by subtraction. |
| Each Set | `74c5b70` | The historical eager prototype. An overwrite publishes the value, static type and installation time together. |

## Results

Values are eight-sample medians. Time is in ns/op. The percentage columns compare
Each Set to the specified control. Comparing with the original control measures
the effect of the historical prototype on its own code base. The comparison with
the current implementation includes intervening GCTime changes.

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

Memory cells show **B/op / allocations per operation**. The read benchmark uses
Go's standard benchmem counters. For writes and mixed pairs, the second figure is
the median fractional malloc count including amortized harness overhead, rounded
to four decimal places. Standard integer allocs/op truncates fractional counts.

## Interpretation

At eight workers, eager warm reads took 11.525 ns/op versus 17.765 for the
current implementation and 8.547 for first-read publication. Against the original
control they fell from 22.740 ns/op to 11.525. The older prototype predates the
entry-local GC task and direct elapsed-time changes, so comparing it with first-read
publication cannot isolate the effect of moving publication from Set to Get.

Every eager overwrite allocated 64 bytes, including the write in an alternating
pair. At one worker, eager overwrites took 167.35 ns versus 108.35 for current.
At eight workers the eager overwrite median fell to 237.05 ns versus 253.75 for
current, with the same extra allocation. Alternating pairs took 219.80 and 317.05
ns for eager at one and eight workers respectively, slower than both current and
first-read publication. These results support retaining the prototype as an experiment.

## Method and reproduction

Apple M3 Pro, Darwin/arm64, Go 1.27.1. Each benchmark used the default clock,
real timers, GCTime of one hour and disabled capacity and MaxAge limits. All
reads used `client.Get(key)`. For warm reads, each worker cycled over a disjoint
stripe of the same 1,024 prewarmed string keys. For writes, workers repeatedly
overwrote one prewarmed key with small integers. One mixed operation includes one
Set and one Get by the same worker. Concurrent pairs can interleave. Parallel
ns/op is wall time divided by completed operations. First-read setup allocations
are outside the timed warm loop.

Export each revision into a separate directory with `git archive`. Obtain the
historical prototype from branch `waterkyuu/perf/atomic-key-publication` if
needed. Run from the repository root in a POSIX shell.

```sh
scratch=$(mktemp -d)
git show e24452d:lazy_read_benchmark_test.go > "$scratch/lazy_read_benchmark_test.go"
for label in current lazy original eager; do
  case "$label" in
    current) rev=bd7714d ;;
    lazy) rev=018eadd ;;
    original) rev=938547f ;;
    eager) rev=74c5b70 ;;
  esac
  mkdir "$scratch/$label"
  git archive -o "$scratch/$label.tar" "$rev"
  tar -xf "$scratch/$label.tar" -C "$scratch/$label"
  go -C "$scratch/$label" test -mod=readonly -c -o "$scratch/$label.test" \
    "$scratch/lazy_read_benchmark_test.go"
done
for repeat in 1 2; do
  for order in 'current lazy eager original' 'lazy original current eager' \
    'original eager lazy current' 'eager current original lazy'; do
    for label in $order; do
      "$scratch/$label.test" -test.run='^$' -test.bench='^BenchmarkLazyGet$' \
        -test.cpu=1,8 -test.benchmem -test.benchtime=300ms >> "$scratch/$label.txt"
    done
  done
done
```

[All 192 measurements](eager-snapshot-darwin-arm64.txt) · [Previous first-read experiment](LAZY_GET_RESULTS.md)
