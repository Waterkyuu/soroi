# Get and Set baseline measurement criteria

Measure warm reads with the public, untyped `client.Get(key)` and overwrites
with `soroi.Set(client, key, value)`. Run Get and Set separately for every
combination below, for 12 Get cases and 12 Set cases per revision.

| Key distribution | Default options | MaxAge only | GCTime only |
| --- | --- | --- | --- |
| 1,024 distinct keys distributed by the client's default hash | 1 and 8 workers | 1 and 8 workers | 1 and 8 workers |
| One hot key shared by all workers | 1 and 8 workers | 1 and 8 workers | 1 and 8 workers |

- Use one client per case. The default policy uses `Options{}`. MaxAge only uses
  `Options{MaxAge: time.Hour}`. GCTime only uses `Options{GCTime: time.Hour}`.
  Use the default clock and leave all other options at their defaults.
- Prepare string keys and values, populate the cache, and read every key once
  before timing. For 1,024 keys, assign each worker a disjoint stripe and cycle
  through it. For one hot key, all workers access the same key.
- In the Get loop, count one successful read per operation. In the Set loop,
  count one successful overwrite of an existing key per operation. Use two
  prepared values of the same type and check each Set error. Verify the stored
  values after stopping the timer so readback is excluded from Set time.
- Use `testing.B.RunParallel` with its default parallelism and `-cpu=1,8`.
  Record ns/op, B/op and allocs/op for all 24 cases, including zeros. Parallel
  ns/op is elapsed time divided by total completed operations across all workers.
- For a comparison, use the same harness, Go version, machine, run duration
  and sample count for both revisions. Run the binaries sequentially in
  alternating order. Report the median for each case and keep the raw samples.
