package soroi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// queryView demonstrates caller-owned aggregation rather than a library-defined readiness policy.
type queryView struct {
	// Ready counts positions with usable data independently of their errors.
	Ready int
	// Errors preserves the caller's chosen error collection shape.
	Errors []error
}

// TestQueriesWithCombine preserves a statically typed caller-defined result for reads and updates.
func TestQueriesWithCombine(t *testing.T) {
	client := newInvalidationClient(t)
	failure := errors.New("offline")
	first := Definition[int]{Key: "number", Fetcher: func(context.Context) (int, error) { return 42, nil }}
	second := Definition[string]{Key: "name", Fetcher: func(context.Context) (string, error) { return "", failure }}
	combine := func(results []Snapshot[any]) queryView {
		_ = client.Stats()
		view := queryView{Errors: []error{}}
		for _, result := range results {
			if result.HasData {
				view.Ready++
			}
			if result.Err != nil {
				view.Errors = append(view.Errors, result.Err)
			}
		}
		return view
	}
	group := QueriesWithCombine(
		client,
		combine,
		first,
		second,
	)
	t.Cleanup(group.Close)
	state := awaitQueries(t, group, func(s queryView) bool { return s.Ready == 1 && len(s.Errors) == 1 })
	if !errors.Is(state.Errors[0], failure) {
		t.Fatalf("custom errors = %v", state.Errors)
	}
	current := group.Snapshot()
	if current.Ready != 1 || len(current.Errors) != 1 {
		t.Fatalf("typed snapshot = %+v", current)
	}
	Set(client, "name", "Alice")
	state = awaitQueries(t, group, func(s queryView) bool { return s.Ready == 2 && len(s.Errors) == 0 })
	group.Close()
	for range group.Updates() {
	}
	if state.Ready != group.Snapshot().Ready {
		t.Fatal("released combined handle lost readable cached results")
	}
}

// TestQueriesCombineOutsideLock isolates a blocked callback from cache writes and shutdown.
func TestQueriesCombineOutsideLock(t *testing.T) {
	client := newInvalidationClient(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	definition := Definition[int]{
		Key:     "counter",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true},
	}
	group := QueriesWithCombine(client, func(results []Snapshot[any]) int {
		once.Do(func() {
			close(entered)
			<-release
		})
		_ = client.Stats()
		if results[0].HasData {
			return results[0].Data.(int)
		}
		return -1
	}, definition)
	<-entered
	done := make(chan error, 1)
	go func() {
		done <- Set(client, "counter", 42)
		group.Close()
		close(done)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("combine blocked client writes")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("combine blocked subscription release")
	}
	unblock.Do(func() { close(release) })
	for range group.Updates() {
	}
}

// TestQueriesCombineTerminalInputs projects empty and failed inputs before ending their streams.
func TestQueriesCombineTerminalInputs(t *testing.T) {
	for _, definitions := range [][]QuerySpec{{}, {Definition[int]{Key: "missing"}}} {
		client := newInvalidationClient(t)
		group := QueriesWithCombine(client, func(results []Snapshot[any]) int { return len(results) }, definitions...)
		state, open := <-group.Updates()
		if !open || state != len(definitions) || group.Snapshot() != state {
			t.Fatalf("terminal projection = %d; open=%v", state, open)
		}
		if _, open := <-group.Updates(); open {
			t.Fatal("terminal combined stream remained open")
		}
		group.Close()
	}
}

// TestQueriesCombineClientClosure lets a callback close its Client without reentering the cache lock.
func TestQueriesCombineClientClosure(t *testing.T) {
	client := newInvalidationClient(t)
	group := QueriesWithCombine(client, func(results []Snapshot[any]) int {
		client.Close()
		return len(results)
	}, Definition[int]{
		Key:     "idle",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true},
	})
	if value := <-group.Updates(); value != 1 {
		t.Fatalf("callback closure result = %d", value)
	}
	for range group.Updates() {
	}
	group.Close()
}

// TestQueriesCombineNil rejects an unusable callback before creating subscriptions or starting work.
func TestQueriesCombineNil(t *testing.T) {
	client := newInvalidationClient(t)
	defer func() {
		if recover() == nil {
			t.Fatal("nil combine was accepted")
		}
		if stats := client.Stats(); stats.CacheEntries != 0 || stats.Loads != 0 {
			t.Fatalf("nil combine started work: %+v", stats)
		}
	}()
	QueriesWithCombine(client, (func([]Snapshot[any]) int)(nil), Definition[int]{
		Key:     "unused",
		Fetcher: func(context.Context) (int, error) { return 1, nil },
	})
}
