package soroi

import (
	"testing"
	"time"
)

// TestGetRetainedValues verifies that value reads preserve zero and nil data and keep stale data available.
func TestGetRetainedValues(t *testing.T) {
	client := NewClient(Options{})
	t.Cleanup(client.Close)
	if err := Set(client, "zero", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[int](client, "zero"); !ok || value != 0 {
		t.Fatalf("typed zero = (%v, %v)", value, ok)
	}
	if value, ok := client.Get("zero"); !ok || value != 0 {
		t.Fatalf("untyped zero = (%v, %v)", value, ok)
	}
	if state := GetState[int](client, "zero"); !state.Stale || !state.HasData {
		t.Fatalf("retained zero state = %+v", state)
	}
	if err := Set[*int](client, "nil", nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[*int](client, "nil"); !ok || value != nil {
		t.Fatalf("typed nil = (%v, %v)", value, ok)
	}
	if err := Set[any](client, "nil-interface", nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := client.Get("nil-interface"); !ok || value != nil {
		t.Fatalf("untyped nil = (%v, %v)", value, ok)
	}
}

// TestGetUnavailableValues reports unreadable keys without exposing a value or binding a missing key.
func TestGetUnavailableValues(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "typed", 42); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[string](client, "typed"); ok || value != "" {
		t.Fatalf("type mismatch = (%v, %v)", value, ok)
	}
	for _, key := range []any{"missing", nil, []int{1}} {
		if value, ok := Get[int](client, key); ok || value != 0 {
			t.Fatalf("typed read of unavailable key %T = (%v, %v)", key, value, ok)
		}
		if value, ok := client.Get(key); ok || value != nil {
			t.Fatalf("untyped read of unavailable key %T = (%v, %v)", key, value, ok)
		}
	}
	if err := Set(client, "missing", "now present"); err != nil {
		t.Fatalf("reading a missing key bound its type: %v", err)
	}
	client.Close()
	if value, ok := Get[int](client, "typed"); ok || value != 0 {
		t.Fatalf("closed read = (%v, %v)", value, ok)
	}
}

// TestGetSkipsFreshnessClock verifies that ordinary value reads avoid freshness calculation.
func TestGetSkipsFreshnessClock(t *testing.T) {
	calls := 0
	client := NewClient(Options{
		StaleTime: time.Hour,
		Clock: func() time.Time {
			calls++
			return time.Unix(0, 0)
		},
	})
	t.Cleanup(client.Close)
	if err := Set(client, "key", "value"); err != nil {
		t.Fatal(err)
	}
	before := calls
	if value, ok := Get[string](client, "key"); !ok || value != "value" {
		t.Fatalf("typed read = (%v, %v)", value, ok)
	}
	if value, ok := client.Get("key"); !ok || value != "value" {
		t.Fatalf("untyped read = (%v, %v)", value, ok)
	}
	if calls != before {
		t.Fatalf("Get called the clock %d times", calls-before)
	}
}

// TestGetTracksRecency verifies that value reads participate in capacity eviction.
func TestGetTracksRecency(t *testing.T) {
	client := NewClient(Options{MaxEntries: 2})
	t.Cleanup(client.Close)
	for _, key := range []string{"a", "b"} {
		if err := Set(client, key, key); err != nil {
			t.Fatal(err)
		}
	}
	if value, ok := Get[string](client, "a"); !ok || value != "a" {
		t.Fatalf("pre-eviction read = (%v, %v)", value, ok)
	}
	if err := Set(client, "c", "c"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.Get("b"); ok {
		t.Fatal("the least recently read value survived eviction")
	}
	if _, ok := client.Get("a"); !ok {
		t.Fatal("the last value read was evicted")
	}
}
