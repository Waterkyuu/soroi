package soroi_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// BenchmarkLazyGet compares untyped hits, overwrites and alternating calls with exact idle retention.
func BenchmarkLazyGet(b *testing.B) {
	b.Run("Get1024", benchmarkLazyGetMany)
	b.Run("Set", func(b *testing.B) { benchmarkLazyOverwrite(b, false) })
	b.Run("SetGetPair", func(b *testing.B) { benchmarkLazyOverwrite(b, true) })
}

// benchmarkLazyGetMany gives each worker its own stripe of naturally hashed, prewarmed keys.
func benchmarkLazyGetMany(b *testing.B) {
	client := soroi.NewClient(soroi.Options{GCTime: time.Hour})
	b.Cleanup(client.Close)
	keys := make([]any, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%08x", i)
		if err := soroi.Set(client, keys[i], 42); err != nil {
			b.Fatal(err)
		}
		if value, ok := client.Get(keys[i]); !ok || value != 42 {
			b.Fatal("warmup missed a preloaded key")
		}
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > len(keys) {
		b.Skip("requires at least one key per worker")
	}
	var next atomic.Int32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		worker := int(next.Add(1)) - 1
		index := worker
		for pb.Next() {
			if value, ok := client.Get(keys[index]); !ok || value != 42 {
				b.Error("lost a preloaded value")
				return
			}
			index += workers
			if index >= len(keys) {
				index = worker
			}
		}
	})
}

// benchmarkLazyOverwrite measures an overwrite or one Set followed by one untyped Get per operation.
func benchmarkLazyOverwrite(b *testing.B, readBack bool) {
	client := soroi.NewClient(soroi.Options{GCTime: time.Hour})
	b.Cleanup(client.Close)
	key := any("hot")
	if err := soroi.Set(client, key, 42); err != nil {
		b.Fatal(err)
	}
	if got, ok := client.Get(key); !ok || got != 42 {
		b.Fatal("initial value missing")
	}
	workers := runtime.GOMAXPROCS(0)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		value := 42
		for pb.Next() {
			value ^= 1
			if err := soroi.Set(client, key, value); err != nil {
				b.Error(err)
				return
			}
			if readBack {
				got, ok := client.Get(key)
				// Another worker may overwrite between this worker's two calls.
				if !ok || (got != 42 && got != 43) || (workers == 1 && got != value) {
					b.Errorf("unexpected read after Set: %v", got)
					return
				}
			}
		}
	})
	b.StopTimer()
	runtime.ReadMemStats(&after)
	// The standard allocs/op truncates fractions when workers share a published version.
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(b.N), "mallocs/op")
	if got, ok := client.Get(key); !ok || (got != 42 && got != 43) {
		b.Fatal("overwritten value missing")
	}
}
