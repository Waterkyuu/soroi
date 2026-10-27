package soroi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// awaitQueries waits for a combined state with a deadline used only as a deadlock guard.
func awaitQueries(t *testing.T, group *QueriesHandle, matches func(QueriesSnapshot) bool) QueriesSnapshot {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case state, open := <-group.Updates():
			if !open {
				t.Fatal("queries closed before the expected state")
			}
			if matches(state) {
				return state
			}
		case <-timer.C:
			t.Fatal("queries did not publish the expected state")
		}
	}
}

// TestQueriesIndependentResults preserves input order, partial results, and errors without stopping siblings.
func TestQueriesIndependentResults(t *testing.T) {
	client := newInvalidationClient(t)
	firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	failure := errors.New("settings offline")
	first := Definition[string]{Key: "user", Fetcher: func(ctx context.Context) (string, error) {
		close(firstStarted)
		select {
		case <-firstRelease:
			return "Alice", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}}
	second := Definition[int]{Key: "settings", Fetcher: func(ctx context.Context) (int, error) {
		close(secondStarted)
		select {
		case <-secondRelease:
			return 0, failure
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}
	group := Queries(client, first, second)
	t.Cleanup(group.Close)
	<-firstStarted
	<-secondStarted
	state := group.Snapshot()
	if state.HasData || !state.Fetching || state.Err != nil || len(state.Results) != 2 {
		t.Fatalf("initial aggregate = %+v", state)
	}
	close(secondRelease)
	state = awaitQueries(t, group, func(s QueriesSnapshot) bool { return s.Results[1].Err != nil })
	if !errors.Is(state.Err, failure) || !state.Fetching || state.Results[0].Status != Pending {
		t.Fatalf("partial failure = %+v", state)
	}
	close(firstRelease)
	state = awaitQueries(t, group, func(s QueriesSnapshot) bool { return !s.Fetching })
	if state.HasData || !errors.Is(state.Err, failure) || state.Results[0].Data != "Alice" {
		t.Fatalf("completed aggregate = %+v", state)
	}
	if err := Set(client, second.Key, 42); err != nil {
		t.Fatal(err)
	}
	state = <-group.Updates()
	if !state.HasData || state.Err != nil || state.Results[1].Data != 42 {
		t.Fatalf("recovered aggregate = %+v", state)
	}
}

// TestQueriesReusePolicy keeps per-definition freshness, retry, tags, and static type bindings.
func TestQueriesReusePolicy(t *testing.T) {
	client := newInvalidationClient(t)
	if err := Set(client, "count", 7); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	failure := errors.New("temporary failure")
	retries := 1
	definition := Definition[int]{
		Key: "count",
		Fetcher: func(context.Context) (int, error) {
			if calls.Add(1) == 1 {
				return 0, failure
			}
			return 8, nil
		},
		Tags: []string{"dashboard"},
		Options: QueryOptions{
			Disable:    true,
			Retry:      &retries,
			RetryDelay: func(int) time.Duration { return 0 },
		},
	}
	group := Queries(client, definition)
	t.Cleanup(group.Close)
	definition.Options.Disable = false
	definition.Tags[0] = "changed"
	state := <-group.Updates()
	if !state.HasData || state.Fetching || calls.Load() != 0 {
		t.Fatalf("disabled cached aggregate = %+v", state)
	}
	if err := client.Invalidate(QueryFilter{Tags: []string{"dashboard"}}); err != nil {
		t.Fatal(err)
	}
	if state = group.Snapshot(); !state.Results[0].Stale || state.Fetching || calls.Load() != 0 {
		t.Fatalf("disabled invalidation = %+v", state)
	}
	if _, err := Fetch(context.Background(), client, "count", func(context.Context) (int, error) {
		return 9, nil
	}); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, group, func(s QueriesSnapshot) bool { return s.Results[0].Data == 9 })
	if !state.HasData || state.Results[0].Stale {
		t.Fatalf("external load = %+v", state)
	}
	active := Queries(client, definition)
	t.Cleanup(active.Close)
	if err := client.Invalidate("count"); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, active, func(s QueriesSnapshot) bool { return s.Results[0].Data == 8 && !s.Fetching })
	if state.Err != nil || !state.HasData || calls.Load() != 2 {
		t.Fatalf("retried aggregate = %+v; calls=%d", state, calls.Load())
	}
	if err := Set(client, "count", "wrong type"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("aggregate lost static binding: %v", err)
	}
}

// TestQueriesDuplicateKeys shares one load while preserving independently configured result positions.
func TestQueriesDuplicateKeys(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	definition := Definition[int]{Key: "shared", Fetcher: func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}
	definitions := []QuerySpec{definition, definition}
	group := Queries(client, definitions...)
	t.Cleanup(group.Close)
	definitions[0] = Definition[string]{Key: "unrelated"}
	<-started
	ordinary := definition.Query(client)
	t.Cleanup(ordinary.Close)
	close(release)
	state := awaitQueries(t, group, func(s QueriesSnapshot) bool { return s.HasData && !s.Fetching })
	if calls.Load() != 1 || state.Results[0].Data != 42 || state.Results[1].Data != 42 {
		t.Fatalf("shared results = %+v; calls=%d", state, calls.Load())
	}
	state.Results[0].Data = "caller-owned slice"
	if current := group.Snapshot(); current.Results[0].Data != 42 {
		t.Fatal("changing a returned result slice changed the group")
	}
	group.Close()
	if err := Set(client, "shared", 43); err != nil {
		t.Fatal(err)
	}
	if ordinary.Snapshot().Data != 43 || group.Snapshot().Results[0].Data != 43 {
		t.Fatal("group closure changed ordinary subscription or cached reads")
	}
}

// TestQueriesLatestUpdates keeps only the latest state while isolating previously returned result slices.
func TestQueriesLatestUpdates(t *testing.T) {
	client := newInvalidationClient(t)
	definition := Definition[int]{
		Key:     "first",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true},
	}
	second := definition
	second.Key = "second"
	group := Queries(client, definition, second)
	t.Cleanup(group.Close)
	initial := <-group.Updates()
	for value := range 100 {
		Set(client, "first", value)
	}
	state := <-group.Updates()
	if state.Results[0].Data != 99 || initial.Results[0].HasData {
		t.Fatalf("latest aggregate = %+v; initial = %+v", state, initial)
	}
	state.Results[0].Data = "changed by caller"
	Set(client, "second", 42)
	state = <-group.Updates()
	if state.Results[0].Data != 99 || state.Results[1].Data != 42 || !state.HasData {
		t.Fatalf("returned slice changed later publications: %+v", state)
	}
}

// TestQueriesValidation reports each construction failure without preventing valid siblings from loading.
func TestQueriesValidation(t *testing.T) {
	client := newInvalidationClient(t)
	firstFailure, secondFailure := errors.New("first failed"), errors.New("second failed")
	group := Queries(client,
		Definition[int]{Key: []string{"invalid"}, Fetcher: func(context.Context) (int, error) { return 0, nil }},
		Definition[int]{Key: "no-fetcher"},
		Definition[int]{Key: "valid", Fetcher: func(context.Context) (int, error) { return 42, nil }},
		Definition[string]{Key: "valid", Fetcher: func(context.Context) (string, error) { return "", nil }},
		Definition[int]{Key: "failure-1", Fetcher: func(context.Context) (int, error) { return 0, firstFailure }},
		Definition[int]{Key: "failure-2", Fetcher: func(context.Context) (int, error) { return 0, secondFailure }},
		nil,
		(*Definition[int])(nil),
	)
	t.Cleanup(group.Close)
	state := awaitQueries(t, group, func(s QueriesSnapshot) bool { return !s.Fetching })
	wants := []error{
		ErrInvalidKey, ErrNoFetcher, nil, ErrTypeMismatch, firstFailure, secondFailure, ErrNoFetcher, ErrNoFetcher,
	}
	for index, want := range wants {
		if !errors.Is(state.Results[index].Err, want) {
			t.Fatalf("result %d = %+v, want %v", index, state.Results[index], want)
		}
		if want != nil && !errors.Is(state.Err, want) {
			t.Fatalf("aggregate error %v does not contain %v", state.Err, want)
		}
	}
	if state.Results[2].Data != 42 || state.HasData {
		t.Fatalf("valid sibling = %+v", state)
	}
}

// TestQueriesEmptyAndFailedLifecycle closes groups with no active subscriptions after publishing an initial state.
func TestQueriesEmptyAndFailedLifecycle(t *testing.T) {
	for _, count := range []int{0, 1} {
		t.Run(map[int]string{0: "empty", 1: "invalid"}[count], func(t *testing.T) {
			client := newInvalidationClient(t)
			definitions := make([]QuerySpec, count)
			if count > 0 {
				definitions[0] = Definition[int]{Key: "missing-fetcher"}
			}
			group := Queries(client, definitions...)
			state, open := <-group.Updates()
			if !open || state.Results == nil || len(state.Results) != count || state.HasData != (count == 0) {
				t.Fatalf("initial terminal group = %+v; open=%v", state, open)
			}
			if _, open := <-group.Updates(); open {
				t.Fatal("group with no active subscriptions left updates open")
			}
			group.Close()
			group.Close()
		})
	}
}

// TestQueriesPollingAndClose reuses copied polling policies and releases only the group's schedules.
func TestQueriesPollingAndClose(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	var calls atomic.Int32
	definition := Definition[int]{
		Key:     "progress",
		Fetcher: func(context.Context) (int, error) { return int(calls.Add(1)), nil },
		Options: QueryOptions{RefetchInterval: time.Second},
	}
	group := Queries(client, definition)
	t.Cleanup(group.Close)
	awaitQueries(t, group, func(s QueriesSnapshot) bool { return s.HasData && !s.Fetching })
	first := nextPollingTimer(t, timers)
	definition.Options.RefetchInterval = time.Hour
	first.fire()
	state := awaitQueries(t, group, func(s QueriesSnapshot) bool { return s.Results[0].Data == 2 && !s.Fetching })
	if state.Err != nil || !state.HasData {
		t.Fatalf("polled aggregate = %+v", state)
	}
	next := nextPollingTimer(t, timers)
	if next.delay != time.Second {
		t.Fatal("definition mutation changed the group's interval")
	}
	group.Close()
	group.Close()
	next.fire()
	assertNoPollingTimer(t, timers)
	if !next.stopped.Load() || calls.Load() != 2 {
		t.Fatal("group closure did not release its polling subscription")
	}
}

// TestQueriesRetainedDataAndCancellation observes failed refreshes and cancellation through existing client APIs.
func TestQueriesRetainedDataAndCancellation(t *testing.T) {
	client := newInvalidationClient(t)
	Set(client, "cached", 7)
	started := make(chan struct{})
	failure := errors.New("offline")
	group := Queries(client,
		Definition[int]{Key: "cached", Fetcher: func(context.Context) (int, error) { return 0, failure }},
		Definition[string]{Key: "pending", Fetcher: func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		}},
	)
	t.Cleanup(group.Close)
	<-started
	if err := client.Invalidate("cached"); err != nil {
		t.Fatal(err)
	}
	state := awaitQueries(t, group, func(s QueriesSnapshot) bool { return errors.Is(s.Err, failure) })
	if !state.Results[0].HasData || state.Results[0].Data != 7 || !state.Fetching {
		t.Fatalf("retained refresh failure = %+v", state)
	}
	if err := client.Cancel("pending"); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, group, func(s QueriesSnapshot) bool { return !s.Fetching })
	if state.Results[1].Status != Idle || state.Results[1].Err != nil || !errors.Is(state.Err, failure) {
		t.Fatalf("canceled aggregate = %+v", state)
	}
	Set(client, "pending", "ready")
	state = <-group.Updates()
	if !state.HasData || !errors.Is(state.Err, failure) {
		t.Fatalf("available data with a refresh error = %+v", state)
	}
}

// TestQueriesCloseDuringLoad releases the stream without canceling a client-owned background load.
func TestQueriesCloseDuringLoad(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	definition := Definition[int]{Key: "active", Fetcher: func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-release:
			finished <- nil
			return 42, nil
		case <-ctx.Done():
			finished <- ctx.Err()
			return 0, ctx.Err()
		}
	}}
	group := Queries(client, definition)
	<-started
	group.Close()
	for range group.Updates() {
	}
	ordinary := definition.Query(client)
	t.Cleanup(ordinary.Close)
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("group closure canceled shared work: %v", err)
	}
	awaitState(t, ordinary.Updates(), func(s Snapshot[int]) bool { return s.Data == 42 && !s.Fetching })
}

// TestQueriesClientClose ends all combined streams even when members have no pending requests.
func TestQueriesClientClose(t *testing.T) {
	client := newInvalidationClient(t)
	definition := Definition[int]{
		Key:     "idle",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true},
	}
	group := Queries(client, definition, definition)
	client.Close()
	for range group.Updates() {
	}
	if state := group.Snapshot(); !errors.Is(state.Err, ErrClosed) || state.Fetching {
		t.Fatalf("closed client aggregate = %+v", state)
	}
	group.Close()
	failed := Queries(client, definition)
	if state := <-failed.Updates(); !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("construction on closed client = %+v", state)
	}
	if _, open := <-failed.Updates(); open {
		t.Fatal("closed client admitted a new group subscription")
	}
}

// TestQueriesMemberFreshness evaluates duplicate definitions independently and expires unavailable data on reads.
func TestQueriesMemberFreshness(t *testing.T) {
	now := time.Unix(0, 0)
	client := NewClient(Options{StaleTime: time.Hour, MaxAge: time.Hour, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	Set(client, "data", 42)
	short := time.Minute
	first := Definition[int]{
		Key:     "data",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true, StaleTime: &short},
	}
	second := first
	second.Options.StaleTime = nil
	group := Queries(client, first, second)
	t.Cleanup(group.Close)
	<-group.Updates()
	short = 0
	now = now.Add(time.Minute)
	state := group.Snapshot()
	if !state.HasData || !state.Results[0].Stale || state.Results[1].Stale {
		t.Fatalf("independent freshness = %+v", state)
	}
	now = now.Add(time.Hour)
	state = group.Snapshot()
	if state.HasData || state.Results[0].HasData || state.Results[1].HasData {
		t.Fatalf("maximum age was ignored: %+v", state)
	}
	state = <-group.Updates()
	if state.HasData || state.Results[0].HasData || state.Results[1].HasData {
		t.Fatalf("maximum age was not published: %+v", state)
	}
}

// TestQueriesConcurrentClosure serializes publications, reads, and repeated group or client shutdown.
func TestQueriesConcurrentClosure(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		client := newInvalidationClient(t)
		group := Queries(client, Definition[int]{
			Key:     "counter",
			Fetcher: func(context.Context) (int, error) { return 0, nil },
			Options: QueryOptions{Disable: true},
		})
		var workers sync.WaitGroup
		for worker := range 4 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for range 20 {
					switch worker {
					case 0:
						_ = Set(client, "counter", 1)
					case 1:
						_ = group.Snapshot()
					case 2:
						group.Close()
					case 3:
						if closeClient {
							client.Close()
						}
					}
				}
			}()
		}
		workers.Wait()
		for range group.Updates() {
		}
		if closeClient && !errors.Is(group.Snapshot().Err, ErrClosed) {
			t.Fatal("closed client was not reported by group snapshots")
		}
	}
}
