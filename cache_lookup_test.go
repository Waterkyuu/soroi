package soroi_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// TestSingleLookupMaintenance rejects stale entry reuse after read-side maintenance removes its binding.
func TestSingleLookupMaintenance(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		name := "retained-binding"
		if capacity > 0 {
			name = "released-binding"
		}
		t.Run(name, func(t *testing.T) {
			installed := time.Unix(100, 0)
			now := installed
			client := soroi.NewClient(soroi.Options{
				StaleTime: time.Hour, MaxAge: time.Minute, MaxEntries: capacity,
				Clock: func() time.Time { return now },
			})
			t.Cleanup(client.Close)
			if err := soroi.Set(client, "key", 42); err != nil {
				t.Fatal(err)
			}
			now = installed.Add(2 * time.Minute)
			// Admission failure must not trigger maintenance or lose the original value's type.
			if s := soroi.Get[string](client, "key"); !errors.Is(s.Err, soroi.ErrTypeMismatch) || s.HasData {
				t.Fatalf("type mismatch = %+v", s)
			}
			if got := client.Stats().AgeExpirations; got != 0 {
				t.Fatalf("rejected read expired data: %d", got)
			}
			s := soroi.Get[int](client, "key")
			if s.HasData || !s.Stale || s.Err != nil || s.Status != soroi.Idle || s.Fetching {
				t.Fatalf("expired read = %+v", s)
			}
			if got := client.Stats().AgeExpirations; got != 1 {
				t.Fatalf("expiration count = %d", got)
			}
			if capacity > 0 {
				if !s.UpdatedAt.IsZero() {
					t.Fatalf("removed entry leaked timestamp: %v", s.UpdatedAt)
				}
				if err := soroi.Set(client, "key", "replacement"); err != nil {
					t.Fatalf("released binding rejected a new type: %v", err)
				}
				if got := soroi.Get[string](client, "key"); got.Data != "replacement" || !got.HasData {
					t.Fatalf("replacement = %+v", got)
				}
			} else {
				if !s.UpdatedAt.Equal(installed) {
					t.Fatalf("retained timestamp = %v", s.UpdatedAt)
				}
				if err := soroi.Set(client, "key", "replacement"); !errors.Is(err, soroi.ErrTypeMismatch) {
					t.Fatalf("retained binding lost its type: %v", err)
				}
			}
		})
	}
}
