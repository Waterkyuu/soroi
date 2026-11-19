package soroi_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// BenchmarkCacheBaseline measures warm public value reads and overwrites across cache policies.
func BenchmarkCacheBaseline(b *testing.B) {
	policies := []struct {
		name    string
		options soroi.Options
	}{
		{"Default", soroi.Options{}},
		{"MaxAge", soroi.Options{MaxAge: time.Hour}},
		{"GCTime", soroi.Options{GCTime: time.Hour}},
	}
	for _, operation := range []string{"Get", "Set"} {
		for _, keys := range []int{1, 1024} {
			for _, policy := range policies {
				name := fmt.Sprintf("%s/Keys=%d/%s", operation, keys, policy.name)
				b.Run(name, func(b *testing.B) {
					measureCacheBaseline(b, operation, keys, policy.options)
				})
			}
		}
	}
}

// measureCacheBaseline gives each worker a stripe of prepared keys or the same shared hot key.
func measureCacheBaseline(b *testing.B, operation string, keyCount int, options soroi.Options) {
	client := soroi.NewClient(options)
	b.Cleanup(client.Close)
	values := [2]int{42, 43}
	workers := runtime.GOMAXPROCS(0)
	stripes := make([][]any, workers)
	keys := make([]any, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
		if err := soroi.Set(client, keys[i], values[0]); err != nil {
			b.Fatal(err)
		}
		if value, ok := client.Get(keys[i]); !ok || value != values[0] {
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
				if value, ok := client.Get(stripe[index]); !ok || value != values[0] {
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
			if err := soroi.Set(client, stripe[index], values[generation]); err != nil {
				b.Error(err)
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
		if value, ok := client.Get(key); !ok || (value != values[0] && value != values[1]) {
			b.Fatalf("unexpected stored value for %v: %v", key, value)
		}
	}
}
