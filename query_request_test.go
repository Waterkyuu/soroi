package soroi

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCachedFetcherAllocations keeps reusable typed loaders off the heap on fresh-cache reads.
func TestCachedFetcherAllocations(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "cached", 42); err != nil {
		t.Fatal(err)
	}
	loader := func(context.Context) (int, error) {
		t.Error("fresh cache invoked the loader")
		return 0, nil
	}
	allocations := testing.AllocsPerRun(100, func() {
		value, err := Fetch(context.Background(), client, "cached", loader)
		if err != nil || value != 42 {
			t.Fatalf("cached fetch = %d, %v", value, err)
		}
	})
	if allocations != 0 {
		t.Fatalf("fresh cached Fetch allocated %.1f objects per read", allocations)
	}
}

// TestConcurrentCachedReads keeps fresh data and exact hit statistics during parallel reads and writes.
func TestConcurrentCachedReads(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "cached", 42); err != nil {
		t.Fatal(err)
	}
	loader := func(context.Context) (int, error) {
		t.Error("fresh concurrent read reached the source")
		return 0, nil
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				value, err := Fetch(context.Background(), client, "cached", loader)
				if err != nil || value != 42 || Get[int](client, "cached").Data != 42 {
					t.Errorf("concurrent cached fetch = %d, %v", value, err)
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for range 100 {
			if err := Set(client, "cached", 42); err != nil {
				t.Error(err)
				return
			}
			_ = client.Stats()
		}
	}()
	close(start)
	workers.Wait()
	if state := client.Stats(); state.CacheHits != 800 || state.CacheMisses != 0 {
		t.Fatalf("concurrent lookup statistics = %+v", state)
	}
}

// TestCachedReadDiagnostics retains ordered hit events when an otherwise read-only client is observed.
func TestCachedReadDiagnostics(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "cached", 42); err != nil {
		t.Fatal(err)
	}
	stream, err := client.SubscribeEvents(EventOptions{Key: "cached", Buffer: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for range 10 {
		if _, err := Fetch[int](context.Background(), client, "cached", nil); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		select {
		case event := <-stream.Events():
			if event.Kind != EventCacheHit {
				t.Fatalf("cached read event = %+v", event)
			}
		default:
			t.Fatal("observed cache hit did not publish its diagnostic event")
		}
	}
	if state := client.Stats(); state.CacheHits != 10 {
		t.Fatalf("observed lookup statistics = %+v", state)
	}
}
