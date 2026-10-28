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
func awaitQueries[V any](t *testing.T, group *QueriesHandle[V], matches func(V) bool) V {
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

// queryResultsSettled reports request completion without imposing data or error aggregation.
func queryResultsSettled(results []Snapshot[any]) bool {
	for _, result := range results {
		if result.Fetching {
			return false
		}
	}
	return true
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
	if len(state) != 2 {
		t.Fatalf("result count = %d", len(state))
	}
	missing := !state[0].HasData && !state[1].HasData
	loading := state[0].Fetching && state[1].Fetching
	if !missing || !loading {
		t.Fatalf("initial aggregate = %+v", state)
	}
	close(secondRelease)
	state = awaitQueries(t, group, func(s []Snapshot[any]) bool { return s[1].Err != nil })
	if !errors.Is(state[1].Err, failure) || !state[0].Fetching || state[0].Status != Pending {
		t.Fatalf("partial failure = %+v", state)
	}
	close(firstRelease)
	state = awaitQueries(t, group, queryResultsSettled)
	if state[1].HasData || !errors.Is(state[1].Err, failure) || state[0].Data != "Alice" {
		t.Fatalf("completed aggregate = %+v", state)
	}
	if err := Set(client, second.Key, 42); err != nil {
		t.Fatal(err)
	}
	state = <-group.Updates()
	available := state[0].HasData && state[1].HasData
	succeeded := state[0].Err == nil && state[1].Err == nil
	if !available || !succeeded || state[1].Data != 42 {
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
	if !state[0].HasData || state[0].Fetching || calls.Load() != 0 {
		t.Fatalf("disabled cached aggregate = %+v", state)
	}
	if err := client.Invalidate(QueryFilter{Tags: []string{"dashboard"}}); err != nil {
		t.Fatal(err)
	}
	if state = group.Snapshot(); !state[0].Stale || state[0].Fetching || calls.Load() != 0 {
		t.Fatalf("disabled invalidation = %+v", state)
	}
	if _, err := Fetch(context.Background(), client, "count", func(context.Context) (int, error) {
		return 9, nil
	}); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, group, func(s []Snapshot[any]) bool { return s[0].Data == 9 })
	if !state[0].HasData || state[0].Stale {
		t.Fatalf("external load = %+v", state)
	}
	active := Queries(client, definition)
	t.Cleanup(active.Close)
	if err := client.Invalidate("count"); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, active, func(s []Snapshot[any]) bool { return s[0].Data == 8 && !s[0].Fetching })
	if state[0].Err != nil || !state[0].HasData || calls.Load() != 2 {
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
	state := awaitQueries(t, group, func(s []Snapshot[any]) bool { return s[0].HasData && queryResultsSettled(s) })
	if calls.Load() != 1 || state[0].Data != 42 || state[1].Data != 42 {
		t.Fatalf("shared results = %+v; calls=%d", state, calls.Load())
	}
	state[0].Data = "caller-owned slice"
	if current := group.Snapshot(); current[0].Data != 42 {
		t.Fatal("changing a returned result slice changed the group")
	}
	group.Close()
	if err := Set(client, "shared", 43); err != nil {
		t.Fatal(err)
	}
	if ordinary.Snapshot().Data != 43 || group.Snapshot()[0].Data != 43 {
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
	if state[0].Data != 99 || initial[0].HasData {
		t.Fatalf("latest aggregate = %+v; initial = %+v", state, initial)
	}
	state[0].Data = "changed by caller"
	Set(client, "second", 42)
	state = <-group.Updates()
	available := state[0].HasData && state[1].HasData
	if state[0].Data != 99 || state[1].Data != 42 || !available {
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
	state := awaitQueries(t, group, queryResultsSettled)
	wants := []error{
		ErrInvalidKey, ErrNoFetcher, nil, ErrTypeMismatch, firstFailure, secondFailure, ErrNoFetcher, ErrNoFetcher,
	}
	for index, want := range wants {
		if !errors.Is(state[index].Err, want) {
			t.Fatalf("result %d = %+v, want %v", index, state[index], want)
		}
	}
	if state[2].Data != 42 || !state[2].HasData {
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
			if !open || state == nil || len(state) != count {
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
	awaitQueries(t, group, func(s []Snapshot[any]) bool { return s[0].HasData && queryResultsSettled(s) })
	first := nextPollingTimer(t, timers)
	definition.Options.RefetchInterval = time.Hour
	first.fire()
	state := awaitQueries(t, group, func(s []Snapshot[any]) bool { return s[0].Data == 2 && !s[0].Fetching })
	if state[0].Err != nil || !state[0].HasData {
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
	state := awaitQueries(t, group, func(s []Snapshot[any]) bool { return errors.Is(s[0].Err, failure) })
	if !state[0].HasData || state[0].Data != 7 || !state[1].Fetching {
		t.Fatalf("retained refresh failure = %+v", state)
	}
	if err := client.Cancel("pending"); err != nil {
		t.Fatal(err)
	}
	state = awaitQueries(t, group, queryResultsSettled)
	if state[1].Status != Idle || state[1].Err != nil || !errors.Is(state[0].Err, failure) {
		t.Fatalf("canceled aggregate = %+v", state)
	}
	Set(client, "pending", "ready")
	state = <-group.Updates()
	if !state[0].HasData || !state[1].HasData || !errors.Is(state[0].Err, failure) {
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
	state := group.Snapshot()
	bothClosed := errors.Is(state[0].Err, ErrClosed) && errors.Is(state[1].Err, ErrClosed)
	if !bothClosed || !queryResultsSettled(state) {
		t.Fatalf("closed client aggregate = %+v", state)
	}
	group.Close()
	failed := Queries(client, definition)
	if state := <-failed.Updates(); !errors.Is(state[0].Err, ErrClosed) {
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
	available := state[0].HasData && state[1].HasData
	if !available || !state[0].Stale || state[1].Stale {
		t.Fatalf("independent freshness = %+v", state)
	}
	now = now.Add(time.Hour)
	state = group.Snapshot()
	if state[0].HasData || state[1].HasData {
		t.Fatalf("maximum age was ignored: %+v", state)
	}
	state = <-group.Updates()
	if state[0].HasData || state[1].HasData {
		t.Fatalf("maximum age was not published: %+v", state)
	}
}

// queryScopeStore observes lock ownership while returning an immutable store identity without I/O.
type queryScopeStore struct {
	// SharedInvalidationStore supplies unused remote operations for a disabled subscription.
	SharedInvalidationStore
	// client is reentered for diagnostics only after verifying that its mutex is available.
	client *Client
	// locked records a regression without hanging the test on a non-reentrant mutex.
	locked bool
}

// InvalidationScope verifies that definition resolution permits Client diagnostics without holding Client.mu.
func (s *queryScopeStore) InvalidationScope() string {
	if !s.client.mu.TryLock() {
		s.locked = true
		return "query-scope-test"
	}
	s.client.mu.Unlock()
	_ = s.client.Stats()
	return "query-scope-test"
}

// TestQueriesDefinitionResolution keeps application-owned store callbacks outside the admission lock.
func TestQueriesDefinitionResolution(t *testing.T) {
	client := newInvalidationClient(t)
	store := &queryScopeStore{client: client}
	shared, err := NewSharedCache[int](store, SharedCacheOptions[int]{
		Key: "remote", Format: "number/v1", TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	group := Queries(client, Definition[int]{
		Key:     "number",
		Fetcher: func(context.Context) (int, error) { return 42, nil },
		Shared:  shared,
		Options: QueryOptions{Disable: true},
	})
	t.Cleanup(group.Close)
	if store.locked {
		t.Fatal("store callback ran while holding Client.mu")
	}
}

// TestQueriesPublicationMaxAge drops expired sibling data when another key causes a group publication.
func TestQueriesPublicationMaxAge(t *testing.T) {
	now := time.Unix(0, 0)
	client := NewClient(Options{StaleTime: time.Hour, MaxAge: time.Minute, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	Set(client, "first", 1)
	Set(client, "second", 2)
	definition := Definition[int]{
		Key:     "first",
		Fetcher: func(context.Context) (int, error) { return 0, nil },
		Options: QueryOptions{Disable: true},
	}
	second := definition
	second.Key = "second"
	group := Queries(client, definition, second)
	t.Cleanup(group.Close)
	ordinary := definition.Query(client)
	t.Cleanup(ordinary.Close)
	<-ordinary.Updates()
	<-group.Updates()
	now = now.Add(time.Minute)
	Set(client, "second", 3)
	state := <-group.Updates()
	if state[0].HasData || state[0].Data != nil || !state[0].Stale || state[1].Data != 3 {
		t.Fatalf("publication did not enforce MaxAge: %+v", state)
	}
	if previous := <-ordinary.Updates(); previous.HasData {
		t.Fatalf("age expiration was not shared with ordinary observers: %+v", previous)
	}
	if current := group.Snapshot(); current[0].HasData || current[1].Data != 3 {
		t.Fatalf("read and publication disagree: %+v", current)
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
		if closeClient && !errors.Is(group.Snapshot()[0].Err, ErrClosed) {
			t.Fatal("closed client was not reported by group snapshots")
		}
	}
}
