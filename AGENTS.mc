# Get baseline measurement criteria

Measure warm value reads through the public, untyped `client.Get(key)`. Run
each row below with **1 worker** and **8 workers**, for 12 cases per revision.

| Key distribution | Plain Get | MaxAge only | GCTime only |
| --- | --- | --- | --- |
| 1,024 distinct keys distributed by the client's default hash | 1 and 8 workers | 1 and 8 workers | 1 and 8 workers |
| One hot key shared by all workers | 1 and 8 workers | 1 and 8 workers | 1 and 8 workers |

- Use one client per case. Plain Get uses `Options{}`. MaxAge only uses
  `Options{MaxAge: time.Hour}`. GCTime only uses `Options{GCTime: time.Hour}`.
  Use the default clock and leave all other options at their defaults.
- Prepare string keys and values, populate the cache, and read every key once
  before timing. Count one successful Get as one operation. For 1,024 keys,
  assign each worker a disjoint stripe and cycle through it. For one hot key,
  every worker reads that same key.
- Use `testing.B.RunParallel` with its default parallelism and `-cpu=1,8`.
  Record ns/op, B/op and allocs/op for all 12 cases, including zeros. Parallel
  ns/op is elapsed time divided by total completed reads across all workers.
- For a comparison, use the same harness, Go version, machine, run duration
  and sample count for both revisions. Run the binaries sequentially in
  alternating order. Report the median for each case and keep the raw samples.
