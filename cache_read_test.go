package soroi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestCachedNilLifecycle distinguishes absent nodes from cached nil data across public read transitions.
func TestCachedNilLifecycle(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	ctx := context.Background()
	if state := Get[int](client, "value"); state.Status != Idle || state.HasData || !state.Stale || state.Err != nil {
		t.Fatalf("missing value = %+v", state)
	}
	// Inspecting an absent key must not bind it to int or prevent a later cached nil pointer.
	if err := Set[*int](client, "value", nil); err != nil {
		t.Fatal(err)
	}
	state := Get[*int](client, "value")
	if state.Status != Success || !state.HasData || state.Data != nil || state.Stale || state.Err != nil {
		t.Fatalf("cached nil = %+v", state)
	}
	if value, err := Fetch[*int](ctx, client, "value", nil); value != nil || err != nil {
		t.Fatalf("fresh nil fetch = %v, %v", value, err)
	}
	if _, err := Fetch[int](ctx, client, "value", nil); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("cached nil type mismatch = %v", err)
	}
	if stats := client.Stats(); stats.CacheHits != 1 || stats.Loads != 0 {
		t.Fatalf("fresh nil decisions = %+v", stats)
	}
	if err := client.Invalidate("value", InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	state = Get[*int](client, "value")
	if !state.HasData || state.Data != nil || !state.Stale || state.Err != nil {
		t.Fatalf("invalidated nil = %+v", state)
	}
	want := 42
	value, err := Fetch(ctx, client, "value", func(context.Context) (*int, error) { return &want, nil })
	if value != &want || err != nil {
		t.Fatalf("invalidated nil reload = %v, %v", value, err)
	}
	if err := client.Remove("value"); err != nil {
		t.Fatal(err)
	}
	if state := Get[*int](client, "value"); state.Status != Idle || state.HasData || state.Err != nil {
		t.Fatalf("removed value = %+v", state)
	}
	if value, err := Fetch(
		ctx,
		client,
		"value",
		func(context.Context) (int, error) { return 7, nil },
	); value != 7 ||
		err != nil {
		t.Fatalf("recreated value = %d, %v", value, err)
	}
	client.Close()
	if state := Get[int](client, "value"); state.HasData || !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("closed read = %+v", state)
	}
	if _, err := Fetch[int](ctx, client, "value", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed fetch = %v", err)
	}
}

// TestCachedSnapshotPublication keeps each snapshot coherent while writers replace changing values.
func TestCachedSnapshotPublication(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "value", 1); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 300 {
				state := Get[int](client, "value")
				validValue := state.HasData && state.Data >= 1 && state.Data <= 301
				validState := state.Status == Success && !state.Stale && !state.Fetching && state.Err == nil
				if !validValue || !validState || !state.ExpiresAt.Equal(state.UpdatedAt.Add(time.Hour)) {
					t.Errorf("mixed publication = %+v", state)
					return
				}
				value, err := Fetch[int](context.Background(), client, "value", nil)
				if err != nil || value < 1 || value > 301 {
					t.Errorf("concurrent fresh fetch = %d, %v", value, err)
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for value := 2; value <= 301; value++ {
			if err := Set(client, "value", value); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	close(start)
	workers.Wait()
	if state := Get[int](client, "value"); state.Data != 301 || !state.HasData || state.Stale {
		t.Fatalf("final publication = %+v", state)
	}
	if stats := client.Stats(); stats.CacheHits != 1200 || stats.CacheMisses != 0 || stats.Loads != 0 {
		t.Fatalf("concurrent read decisions = %+v", stats)
	}
}
