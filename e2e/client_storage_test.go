package e2e_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// TestClientConcurrentStorage uses one public Client for parallel, checked multi-key API workflows.
func TestClientConcurrentStorage(t *testing.T) {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	var workers sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			key := fmt.Sprintf("worker:%d", worker)
			for value := 0; value < 50; value++ {
				if err := soroi.Set(client, key, value); err != nil {
					t.Error(err)
					return
				}
				if state := soroi.Get[int](client, key); state.Err != nil || !state.HasData || state.Data != value {
					t.Errorf("read = %+v", state)
					return
				}
				if err := soroi.Update(
					client,
					key,
					func(current int) (int, error) { return current + 1, nil },
				); err != nil {
					t.Error(err)
					return
				}
				if current, err := soroi.Fetch[int](
					context.Background(),
					client,
					key,
					nil,
				); err != nil ||
					current != value+1 {
					t.Errorf("fetch = %d, %v", current, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	if stats := client.Stats(); stats.CacheEntries != 32 || stats.CacheHits != 1600 || stats.Loads != 0 {
		t.Fatalf("aggregate state = %+v", stats)
	}
	if err := client.Invalidate(soroi.QueryFilter{}); err != nil {
		t.Fatal(err)
	}
	for worker := 0; worker < 32; worker++ {
		if state := soroi.Get[int](client, fmt.Sprintf("worker:%d", worker)); !state.HasData || !state.Stale {
			t.Fatalf("global invalidation = %+v", state)
		}
	}
}
