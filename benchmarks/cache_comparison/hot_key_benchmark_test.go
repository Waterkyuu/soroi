package comparison_test

import (
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
	"github.com/maypok86/otter/v2"
)

const (
	// hotKey identifies the same cache entry for every worker.
	hotKey = "hot"
	// hotValue is the initial value and one of two overwrite values.
	hotValue = uint64(42)
	// retentionInterval leaves the entry live during every benchmark sample.
	retentionInterval = time.Hour
)

// BenchmarkAtomicHotKey compares hits and overwrites of one preexisting key.
func BenchmarkAtomicHotKey(b *testing.B) {
	cases := []struct {
		// name identifies the retention policy in benchmark output.
		name string
		// soroi configures only the policy under measurement.
		soroi soroi.Options
		// expiry selects the corresponding policy for Otter.
		expiry otter.ExpiryCalculator[string, uint64]
	}{
		{name: "Pure"},
		{
			name:   "MaxAge",
			soroi:  soroi.Options{MaxAge: retentionInterval},
			expiry: otter.ExpiryWriting[string, uint64](retentionInterval),
		},
		{
			name:   "GCTime",
			soroi:  soroi.Options{GCTime: retentionInterval},
			expiry: otter.ExpiryAccessing[string, uint64](retentionInterval),
		},
	}
	for _, tc := range cases {
		b.Run("Soroi/Get/"+tc.name, func(b *testing.B) {
			client := soroi.NewClient(tc.soroi)
			b.Cleanup(client.Close)
			key := any(hotKey)
			if err := soroi.Set(client, key, hotValue); err != nil {
				b.Fatal(err)
			}
			benchmarkHotRead(b, func() (uint64, bool) { return soroi.Get[uint64](client, key) })
		})
		b.Run("Soroi/Set/"+tc.name, func(b *testing.B) {
			client := soroi.NewClient(tc.soroi)
			b.Cleanup(client.Close)
			key := any(hotKey)
			if err := soroi.Set(client, key, hotValue); err != nil {
				b.Fatal(err)
			}
			benchmarkHotWrite(b, func(value uint64) error { return soroi.Set(client, key, value) })
			requireWrittenValue(b, func() (uint64, bool) { return soroi.Get[uint64](client, key) })
		})
		b.Run("Otter/Get/"+tc.name, func(b *testing.B) {
			cache := otter.Must(&otter.Options[string, uint64]{ExpiryCalculator: tc.expiry})
			b.Cleanup(func() { cache.StopAllGoroutines() })
			cache.Set(hotKey, hotValue)
			benchmarkHotRead(b, func() (uint64, bool) { return cache.GetIfPresent(hotKey) })
		})
		b.Run("Otter/Set/"+tc.name, func(b *testing.B) {
			cache := otter.Must(&otter.Options[string, uint64]{ExpiryCalculator: tc.expiry})
			b.Cleanup(func() { cache.StopAllGoroutines() })
			cache.Set(hotKey, hotValue)
			benchmarkHotWrite(b, func(value uint64) error {
				cache.Set(hotKey, value)
				return nil
			})
			requireWrittenValue(b, func() (uint64, bool) { return cache.GetIfPresent(hotKey) })
		})
	}
}

// benchmarkHotRead verifies that every hit returns the preexisting value.
func benchmarkHotRead(b *testing.B, read func() (uint64, bool)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if value, ok := read(); !ok || value != hotValue {
				b.Errorf("Get = (%d, %t), want (%d, true)", value, ok, hotValue)
				return
			}
		}
	})
	b.StopTimer()
}

// benchmarkHotWrite measures overwrites with no concurrent reads in the timed section.
func benchmarkHotWrite(b *testing.B, write func(uint64) error) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		value := hotValue
		for pb.Next() {
			value ^= 1
			if err := write(value); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
}

// requireWrittenValue verifies outside the timed interval that the key remains readable.
func requireWrittenValue(b *testing.B, read func() (uint64, bool)) {
	b.Helper()
	if value, ok := read(); !ok || (value != hotValue && value != hotValue^1) {
		b.Fatalf("value after Set = (%d, %t)", value, ok)
	}
}
