package comparison_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

// comparisonKeys fixes the populated working set independently of CPU count.
const comparisonKeys = 1024

// accessCache gives both libraries the same timed operation and key-selection overhead.
type accessCache struct {
	// read inspects an existing result without invoking a source loader.
	read func(int) (uint64, bool)
	// fetch reads or loads a result, with source invocations tracked separately.
	fetch func(int) (uint64, error)
	// write replaces one existing result without increasing the working set.
	write func(int) error
	// clear removes the working set before a cold burst.
	clear func()
	// close releases resources owned by the fixture.
	close func()
}

// newAccessCache prepares identical keys, values, freshness windows, and capacity settings.
// Fetch comparisons enable Otter statistics to match Soroi's always-on lookup counters.
func newAccessCache(b *testing.B, library string, capacity int, countStats bool, source func() uint64) accessCache {
	b.Helper()
	keys := make([]string, comparisonKeys)
	boxed := make([]any, comparisonKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
		boxed[i] = keys[i]
	}
	if library == "Soroi" {
		client := soroi.NewClient(soroi.Options{StaleTime: time.Hour, MaxEntries: capacity})
		loader := func(context.Context) (uint64, error) { return source(), nil }
		for _, key := range boxed {
			if err := soroi.Set(client, key, uint64(42)); err != nil {
				b.Fatal(err)
			}
		}
		return accessCache{
			read: func(i int) (uint64, bool) {
				state := soroi.Get[uint64](client, boxed[i])
				return state.Data, state.HasData && state.Err == nil && !state.Stale
			},
			fetch: func(i int) (uint64, error) { return soroi.Fetch(context.Background(), client, boxed[i], loader) },
			write: func(i int) error { return soroi.Set(client, boxed[i], uint64(42)) },
			clear: client.Clear,
			close: client.Close,
		}
	}
	options := &otter.Options[string, uint64]{
		MaximumSize:      capacity,
		ExpiryCalculator: otter.ExpiryWriting[string, uint64](time.Hour),
	}
	if countStats {
		options.StatsRecorder = stats.NewCounter()
	}
	cache := otter.Must(options)
	loader := otter.LoaderFunc[string, uint64](func(context.Context, string) (uint64, error) { return source(), nil })
	for _, key := range keys {
		cache.Set(key, 42)
	}
	return accessCache{
		read:  func(i int) (uint64, bool) { return cache.GetIfPresent(keys[i]) },
		fetch: func(i int) (uint64, error) { return cache.Get(context.Background(), keys[i], loader) },
		write: func(i int) error { cache.Set(keys[i], 42); return nil },
		clear: cache.InvalidateAll,
		close: func() { cache.StopAllGoroutines() },
	}
}

// BenchmarkCacheAccess measures populated reads, loadable hits, updates, and a 15:1 read/write mix.
// Run with -cpu=1,4,8 to measure single-worker overhead and shared-instance contention.
func BenchmarkCacheAccess(b *testing.B) {
	for _, capacity := range []int{0, comparisonKeys} {
		b.Run(fmt.Sprintf("Capacity=%d", capacity), func(b *testing.B) {
			for _, workingSet := range []int{1, comparisonKeys} {
				b.Run(fmt.Sprintf("Keys=%d", workingSet), func(b *testing.B) {
					for _, operation := range []string{"Read", "Fetch", "Write", "Mixed"} {
						b.Run(operation, func(b *testing.B) {
							for _, library := range []string{"Soroi", "Otter"} {
								b.Run(library, func(b *testing.B) {
									benchmarkCacheAccess(b, library, capacity, workingSet, operation)
								})
							}
						})
					}
				})
			}
		})
	}
}

// benchmarkCacheAccess checks every read and verifies no warm operation reaches the source.
func benchmarkCacheAccess(b *testing.B, library string, capacity, workingSet int, operation string) {
	var calls atomic.Uint64
	cache := newAccessCache(b, library, capacity, operation == "Fetch", func() uint64 { calls.Add(1); return 42 })
	defer cache.close()
	var workerID atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := int(workerID.Add(1))
		for pb.Next() {
			index++
			key := index & (workingSet - 1)
			switch {
			case operation == "Write" || operation == "Mixed" && index&15 == 0:
				if err := cache.write(key); err != nil {
					b.Error(err)
					return
				}
			case operation == "Fetch":
				if value, err := cache.fetch(key); err != nil || value != 42 {
					b.Errorf("fetch = %d, %v", value, err)
					return
				}
			default:
				if value, ok := cache.read(key); !ok || value != 42 {
					b.Errorf("read = %d, %v", value, ok)
					return
				}
			}
		}
	})
	b.StopTimer()
	if calls.Load() != 0 {
		b.Fatalf("warm case unexpectedly invoked source %d times", calls.Load())
	}
}

// BenchmarkColdBurst compares eight simultaneous callers loading the same initially absent key.
// One operation is an eight-request burst; scheduling and source hashing are intentionally timed.
func BenchmarkColdBurst(b *testing.B) {
	for _, library := range []string{"Soroi", "Otter"} {
		b.Run(library, func(b *testing.B) {
			payload := make([]byte, 64*1024)
			benchmarkBurst(b, library, func() uint64 {
				_ = sha256.Sum256(payload)
				return 42
			})
		})
	}
}

// benchmarkBurst validates equal eight-request cold bursts and reports physical source calls.
func benchmarkBurst(b *testing.B, library string, source func() uint64) {
	b.Helper()
	var calls atomic.Uint64
	cache := newAccessCache(b, library, 0, true, func() uint64 {
		calls.Add(1)
		return source()
	})
	defer cache.close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache.clear()
		b.StartTimer()
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(8)
		done.Add(8)
		for range 8 {
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				if value, err := cache.fetch(0); err != nil || value != 42 {
					b.Errorf("cold fetch = %d, %v", value, err)
				}
			}()
		}
		ready.Wait()
		close(start)
		done.Wait()
	}
	b.StopTimer()
	if calls.Load() != uint64(b.N) {
		b.Fatalf("source calls = %d, want %d", calls.Load(), b.N)
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "loads/burst")
	b.ReportMetric(8, "reads/burst")
}
