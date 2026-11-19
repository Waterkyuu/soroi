package comparison_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
)

// BenchmarkOtterBaseline mirrors the Soroi baseline with Otter's public hit and overwrite operations.
func BenchmarkOtterBaseline(b *testing.B) {
	policies := []struct {
		// name matches the corresponding Soroi policy in the baseline matrix.
		name string
		// expiry configures when an entry expires after a write or a hit.
		expiry otter.ExpiryCalculator[string, int]
	}{
		{name: "Default"},
		{name: "MaxAge", expiry: otter.ExpiryWriting[string, int](time.Hour)},
		{name: "GCTime", expiry: otter.ExpiryAccessing[string, int](time.Hour)},
	}
	for _, operation := range []string{"Get", "Set"} {
		for _, keys := range []int{1, 1024} {
			for _, policy := range policies {
				name := fmt.Sprintf("%s/Keys=%d/%s", operation, keys, policy.name)
				b.Run(name, func(b *testing.B) {
					measureOtterBaseline(b, operation, keys, policy.expiry)
				})
			}
		}
	}
}

// measureOtterBaseline gives each worker a stripe of prepared keys or the same shared hot key.
func measureOtterBaseline(b *testing.B, operation string, keyCount int, expiry otter.ExpiryCalculator[string, int]) {
	cache := otter.Must(&otter.Options[string, int]{ExpiryCalculator: expiry})
	b.Cleanup(func() { cache.StopAllGoroutines() })
	values := [2]int{42, 43}
	workers := runtime.GOMAXPROCS(0)
	stripes := make([][]string, workers)
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
		if _, inserted := cache.Set(keys[i], values[0]); !inserted {
			b.Fatalf("expected a new key: %v", keys[i])
		}
		if value, ok := cache.GetIfPresent(keys[i]); !ok || value != values[0] {
			b.Fatalf("warmup failed for %v", keys[i])
		}
		stripes[i%workers] = append(stripes[i%workers], keys[i])
	}
	if keyCount == 1 {
		for i := range stripes {
			stripes[i] = keys
		}
	}
	var next atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		stripe := stripes[next.Add(1)-1]
		index := 0
		if operation == "Get" {
			for pb.Next() {
				if value, ok := cache.GetIfPresent(stripe[index]); !ok || value != values[0] {
					b.Errorf("read failed for %v", stripe[index])
					return
				}
				index++
				if index == len(stripe) {
					index = 0
				}
			}
			return
		}
		generation := 1
		for pb.Next() {
			if _, inserted := cache.Set(stripe[index], values[generation]); inserted {
				b.Errorf("expected overwrite for %v", stripe[index])
				return
			}
			index++
			if index == len(stripe) {
				index = 0
				generation ^= 1
			}
		}
	})
	b.StopTimer()
	for _, key := range keys {
		if value, ok := cache.GetIfPresent(key); !ok || (value != values[0] && value != values[1]) {
			b.Fatalf("unexpected stored value for %v: %v", key, value)
		}
	}
}
