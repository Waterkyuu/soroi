package soroi

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// groupScopeKeys chooses distinct storage owners without depending on randomized routing.
func groupScopeKeys(t *testing.T, client *Client, count int) []string {
	t.Helper()
	keys := make([]string, 0, count)
	seen := make(map[*clientBucket]bool)
	for i := 0; i < 100000 && len(keys) < count; i++ {
		key := fmt.Sprintf("group-scope:%d", i)
		bucket := client.bucketFor(key)
		if !seen[bucket] {
			seen[bucket] = true
			keys = append(keys, key)
		}
	}
	if len(keys) != count {
		t.Fatal("could not find distinct owners")
	}
	return keys
}

// groupScopeSubscribe creates disabled subscriptions so measurements never depend on source work.
func groupScopeSubscribe(t *testing.T, client *Client, keys ...string) *QueriesHandle[[]Snapshot[any]] {
	t.Helper()
	definitions := make([]QuerySpec, len(keys))
	for i, key := range keys {
		definitions[i] = Definition[int]{
			Key: key, Fetcher: func(context.Context) (int, error) { return 0, nil },
			Options: QueryOptions{Disable: true},
		}
	}
	group := Queries(client, definitions...)
	t.Cleanup(group.Close)
	<-group.Updates()
	return group
}

// groupScopeWriteWhileBlocked checks independent progress with an explicitly held unrelated bucket.
func groupScopeWriteWhileBlocked(t *testing.T, client *Client, key, unrelated string) {
	t.Helper()
	blocked := client.bucketFor(unrelated)
	blocked.mu.raw.Lock()
	done := make(chan error, 1)
	go func() { done <- Set(client, key, 42) }()
	var err error
	var timedOut bool
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		timedOut = true
	}
	blocked.mu.raw.Unlock()
	if timedOut {
		<-done
		t.Fatal("unrelated bucket blocked grouped write")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestGroupScopeIndependentProgress preserves progress outside overlapping group dependencies.
func TestGroupScopeIndependentProgress(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	keys := groupScopeKeys(t, client, 5)
	first := groupScopeSubscribe(t, client, keys[0], keys[1], keys[0])
	bridge := groupScopeSubscribe(t, client, keys[1], keys[2])
	other := groupScopeSubscribe(t, client, keys[3], keys[4])
	groupScopeWriteWhileBlocked(t, client, keys[0], keys[3])
	states := <-first.Updates()
	if states[0].Data != 42 || states[2].Data != 42 {
		t.Fatalf("duplicate group positions disagree: %+v", states)
	}
	groupScopeWriteWhileBlocked(t, client, keys[3], keys[0])
	if states := <-other.Updates(); states[0].Data != 42 {
		t.Fatalf("independent group did not publish: %+v", states)
	}
	bridge.Close()
	groupScopeWriteWhileBlocked(t, client, keys[0], keys[2])
	first.Close()
	groupScopeWriteWhileBlocked(t, client, keys[0], keys[1])
}

// TestGroupScopeTransitiveExpiry preserves sibling expiration and notifications across overlapping groups.
func TestGroupScopeTransitiveExpiry(t *testing.T) {
	var ticks atomic.Int64
	ticks.Store(time.Hour.Nanoseconds())
	clock := func() time.Time { return time.Unix(0, ticks.Load()) }
	client := NewClient(Options{Clock: clock, StaleTime: time.Hour, MaxAge: time.Minute})
	// This injected clock is concurrency-safe, allowing the test to exercise partial group locks.
	client.concurrentClock = true
	t.Cleanup(client.Close)
	keys := groupScopeKeys(t, client, 4)
	for i, key := range keys {
		if err := Set(client, key, i+1); err != nil {
			t.Fatal(err)
		}
	}
	first := groupScopeSubscribe(t, client, keys[0], keys[1])
	second := groupScopeSubscribe(t, client, keys[1], keys[2])
	ordinary := Query(
		client,
		keys[2],
		func(context.Context) (int, error) { return 0, nil },
		QueryOptions{Disable: true},
	)
	t.Cleanup(ordinary.Close)
	<-ordinary.Updates()
	ticks.Add(time.Minute.Nanoseconds())
	groupScopeWriteWhileBlocked(t, client, keys[0], keys[3])
	if states := <-first.Updates(); states[0].Data != 42 || states[1].HasData {
		t.Fatalf("first group did not expire sibling: %+v", states)
	}
	if states := <-second.Updates(); states[0].HasData || states[1].HasData {
		t.Fatalf("overlapping group retained expired state: %+v", states)
	}
	if state := <-ordinary.Updates(); state.HasData {
		t.Fatalf("ordinary observer retained expired state: %+v", state)
	}
}

// TestGroupScopeConcurrentTopology keeps changing group membership safe during writes and snapshots.
func TestGroupScopeConcurrentTopology(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	keys := groupScopeKeys(t, client, 4)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := range 60 {
				key := keys[worker]
				if worker < 2 {
					if err := Set(client, key, i); err != nil {
						t.Error(err)
					}
					continue
				}
				definition := Definition[int]{
					Key: keys[i%len(keys)], Fetcher: func(context.Context) (int, error) { return 0, nil },
					Options: QueryOptions{Disable: true},
				}
				sibling := definition
				sibling.Key = keys[(i+1)%len(keys)]
				group := Queries(client, definition, sibling, definition)
				state := group.Snapshot()
				if state[0].HasData != state[2].HasData || state[0].Data != state[2].Data {
					t.Error("duplicate members observed different versions")
				}
				group.Close()
				group.Close()
			}
		}()
	}
	close(start)
	workers.Wait()
	client.Close()
}
