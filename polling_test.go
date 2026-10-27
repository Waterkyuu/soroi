package soroi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// pollTestTimer lets tests deliver an interval, including a callback racing with cancellation.
type pollTestTimer struct {
	// delay records the requested interval without depending on elapsed time.
	delay time.Duration
	// fire delivers this timer's callback synchronously.
	fire func()
	// stopped records whether the owner released this timer.
	stopped atomic.Bool
}

// newPollingClient replaces interval timers with explicitly delivered callbacks.
func newPollingClient(t *testing.T, options Options) (*Client, <-chan *pollTestTimer) {
	t.Helper()
	if options.Clock == nil {
		options.Clock = func() time.Time { return time.Unix(0, 0) }
	}
	client := NewClient(options)
	t.Cleanup(client.Close)
	timers := make(chan *pollTestTimer, 32)
	client.pollAfterFunc = func(delay time.Duration, callback func()) func() {
		timer := &pollTestTimer{delay: delay, fire: callback}
		timers <- timer
		return func() { timer.stopped.Store(true) }
	}
	return client, timers
}

// nextPollingTimer bounds harness waits while leaving interval delivery under test control.
func nextPollingTimer(t *testing.T, timers <-chan *pollTestTimer) *pollTestTimer {
	t.Helper()
	select {
	case timer := <-timers:
		return timer
	case <-time.After(3 * time.Second):
		t.Fatal("polling timer was not scheduled")
		return nil
	}
}

// assertNoPollingTimer checks that an operation synchronously left the schedule unchanged.
func assertNoPollingTimer(t *testing.T, timers <-chan *pollTestTimer) {
	t.Helper()
	select {
	case timer := <-timers:
		t.Fatalf("unexpected polling timer: %v", timer.delay)
	default:
	}
}

// TestPollingFreshData refreshes a fresh result through a copied definition policy and publishes its cause.
func TestPollingFreshData(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	var calls atomic.Int32
	definition, err := QueryDefinition("task:42", func(context.Context) (int, error) {
		return int(calls.Add(1)), nil
	}, QueryOptions{RefetchInterval: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	query := definition.Query(client)
	t.Cleanup(query.Close)
	definition.Options.RefetchInterval = time.Minute
	state := awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	if state.Stale {
		t.Fatal("initial result was not fresh")
	}
	timer := nextPollingTimer(t, timers)
	if timer.delay != 3*time.Second {
		t.Fatalf("interval = %v", timer.delay)
	}
	events := subscribeTestEvents(t, client, EventOptions{Key: "task:42", Buffer: 8})
	timer.fire()
	state = awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale || state.Err != nil {
		t.Fatalf("polled result = %+v", state)
	}
	event := awaitEventKind(t, events, EventLoadStarted)
	if event.Trigger != TriggerPoll || event.Trigger.String() != "poll" || event.Reason != ReasonRefetch {
		t.Fatalf("polling event = %+v", event)
	}
	current := nextPollingTimer(t, timers)
	timer.fire()
	assertNoPollingTimer(t, timers)
	query.Close()
	current.fire()
	if !current.stopped.Load() || calls.Load() != 2 {
		t.Fatal("released subscription admitted a late interval")
	}
}

// TestPollingEnablement uses the existing enablement API without canceling unrelated explicit reads.
func TestPollingEnablement(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	var calls atomic.Int32
	load := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
	query := Query(client, "task", load, QueryOptions{Disable: true, RefetchInterval: time.Second})
	t.Cleanup(query.Close)
	assertNoPollingTimer(t, timers)
	if calls.Load() != 0 {
		t.Fatal("disabled construction loaded data")
	}
	if err := query.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	old := nextPollingTimer(t, timers)
	if err := query.SetEnabled(false); err != nil {
		t.Fatal(err)
	}
	old.fire()
	assertNoPollingTimer(t, timers)
	if !old.stopped.Load() || calls.Load() != 1 {
		t.Fatal("disabled subscription continued polling")
	}
	if value, err := query.Refetch(context.Background()); value != 2 || err != nil {
		t.Fatalf("disabled explicit refresh = %d, %v", value, err)
	}
	assertNoPollingTimer(t, timers)
	if err := query.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	current := nextPollingTimer(t, timers)
	current.fire()
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 3 && !s.Fetching })
	if calls.Load() != 3 {
		t.Fatal("reenabling ignored fresh data or failed to resume polling")
	}
}

// TestPollingSharedSchedule chooses the shortest interval and the oldest subscriber when intervals tie.
func TestPollingSharedSchedule(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	Set(client, "task", 0)
	slow := Query(client, "task", func(context.Context) (int, error) { return 1, nil },
		QueryOptions{RefetchInterval: 5 * time.Second})
	t.Cleanup(slow.Close)
	old := nextPollingTimer(t, timers)
	fast := Query(client, "task", func(context.Context) (int, error) { return 2, nil },
		QueryOptions{RefetchInterval: time.Second})
	t.Cleanup(fast.Close)
	current := nextPollingTimer(t, timers)
	if !old.stopped.Load() || current.delay != time.Second {
		t.Fatal("faster subscriber did not replace the shared schedule")
	}
	equal := Query(client, "task", func(context.Context) (int, error) { return 3, nil },
		QueryOptions{RefetchInterval: time.Second})
	t.Cleanup(equal.Close)
	passive := Query(client, "task", func(context.Context) (int, error) { return 4, nil })
	t.Cleanup(passive.Close)
	assertNoPollingTimer(t, timers)
	old.fire()
	current.fire()
	awaitState(t, passive.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	current = nextPollingTimer(t, timers)
	fast.Close()
	if !current.stopped.Load() {
		t.Fatal("closing the selected subscriber did not release its timer")
	}
	current = nextPollingTimer(t, timers)
	current.fire()
	awaitState(t, equal.Updates(), func(s Snapshot[int]) bool { return s.Data == 3 && !s.Fetching })
	current = nextPollingTimer(t, timers)
	equal.SetEnabled(false)
	if !current.stopped.Load() {
		t.Fatal("disabling the selected subscriber did not release its timer")
	}
	current = nextPollingTimer(t, timers)
	if current.delay != 5*time.Second {
		t.Fatalf("remaining interval = %v", current.delay)
	}
	current.fire()
	awaitState(t, passive.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	current = nextPollingTimer(t, timers)
	slow.Close()
	if !current.stopped.Load() {
		t.Fatal("non-polling subscriber kept the shared timer alive")
	}
	current.fire()
	assertNoPollingTimer(t, timers)
}

// TestPollingSkipsActiveLoads drops elapsed intervals without invalidating or queuing behind a slow request.
func TestPollingSkipsActiveLoads(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	var calls atomic.Int32
	release := make(chan struct{})
	query := Query(client, "task", func(ctx context.Context) (int, error) {
		call := calls.Add(1)
		if call == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		return int(call), nil
	}, QueryOptions{RefetchInterval: time.Second})
	t.Cleanup(query.Close)
	for range 4 {
		nextPollingTimer(t, timers).fire()
	}
	if client.Stats().Loads != 1 || client.Stats().MergedRequests != 0 {
		t.Fatalf("intervals admitted work behind the active load: %+v", client.Stats())
	}
	close(release)
	state := awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	if state.Stale || calls.Load() != 1 {
		t.Fatalf("skipped intervals changed the completed result: %+v", state)
	}
	nextPollingTimer(t, timers).fire()
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
}

// TestPollingRecovery preserves previous data after retry exhaustion and retries on a later interval.
func TestPollingRecovery(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	failure := errors.New("temporarily unavailable")
	var calls atomic.Int32
	retries := 1
	query := Query(client, "task", func(context.Context) (int, error) {
		call := int(calls.Add(1))
		if call == 2 || call == 3 {
			return 0, failure
		}
		return call, nil
	}, QueryOptions{RefetchInterval: time.Second, Retry: &retries,
		RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(query.Close)
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	nextPollingTimer(t, timers).fire()
	state := awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Err != nil && !s.Fetching })
	if !errors.Is(state.Err, failure) || state.Data != 1 || !state.HasData || calls.Load() != 3 {
		t.Fatalf("failed refresh = %+v, calls=%d", state, calls.Load())
	}
	nextPollingTimer(t, timers).fire()
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 4 && !s.Fetching })
}

// TestPollingExecutionBudget applies the selected subscription's budget and recovers after overload.
func TestPollingExecutionBudget(t *testing.T) {
	budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	Set(client, "task", 1)
	query := Query(client, "task", func(context.Context) (int, error) { return 2, nil },
		QueryOptions{RefetchInterval: time.Second, Executor: budget})
	t.Cleanup(query.Close)
	_, release, err := budget.acquireContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	nextPollingTimer(t, timers).fire()
	state := awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Err != nil && !s.Fetching })
	if !errors.Is(state.Err, ErrQueueFull) || state.Data != 1 {
		t.Fatalf("overloaded refresh = %+v", state)
	}
	release()
	nextPollingTimer(t, timers).fire()
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
}

// TestPollingOptIn leaves imperative reads, invalid handles, and nonpositive intervals without timers.
func TestPollingOptIn(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	load := func(context.Context) (int, error) { return 1, nil }
	for _, interval := range []time.Duration{0, -time.Second} {
		query := Query(client, interval, load, QueryOptions{RefetchInterval: interval})
		awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
		query.Close()
	}
	options := QueryOptions{RefetchInterval: time.Second}
	Fetch(context.Background(), client, "fetch", load, options)
	Prefetch(context.Background(), client, "prefetch", load, options)
	query := Query(client, []int{1}, load, options)
	if !errors.Is(query.Snapshot().Err, ErrInvalidKey) {
		t.Fatal("invalid construction was accepted")
	}
	query.Close()
	assertNoPollingTimer(t, timers)
}

// TestPollingCacheOperations keeps live polling subscriptions usable after cache replacement or removal.
func TestPollingCacheOperations(t *testing.T) {
	for _, operation := range []string{"set", "remove", "clear"} {
		t.Run(operation, func(t *testing.T) {
			client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
			Set(client, "task", 1)
			query := Query(client, "task", func(context.Context) (int, error) { return 2, nil },
				QueryOptions{RefetchInterval: time.Second})
			t.Cleanup(query.Close)
			timer := nextPollingTimer(t, timers)
			switch operation {
			case "set":
				Set(client, "task", 3)
			case "remove":
				client.Remove("task")
			case "clear":
				client.Clear()
			}
			timer.fire()
			awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
		})
	}
}

// TestHTTPPollingProgress exercises real HTTP reads and shared updates using a controlled interval source.
func TestHTTPPollingProgress(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		progress := 20
		if requests.Add(1) > 1 {
			progress = 50
		}
		json.NewEncoder(w).Encode(progress)
	}))
	t.Cleanup(server.Close)
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	load := func(ctx context.Context) (int, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err != nil {
			return 0, err
		}
		response, err := server.Client().Do(request)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		var progress int
		err = json.NewDecoder(response.Body).Decode(&progress)
		return progress, err
	}
	query := Query(client, "task:42", load, QueryOptions{RefetchInterval: 3 * time.Second})
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.Data == 20 && !s.Fetching })
	passive := Query(client, "task:42", load)
	t.Cleanup(passive.Close)
	nextPollingTimer(t, timers).fire()
	awaitState(t, passive.Updates(), func(s Snapshot[int]) bool { return s.Data == 50 && !s.Fetching })
	current := nextPollingTimer(t, timers)
	query.Close()
	current.fire()
	if !current.stopped.Load() || requests.Load() != 2 {
		t.Fatalf("closed view left polling active: requests=%d", requests.Load())
	}
	assertNoPollingTimer(t, timers)
}

// TestPollingClientClose cancels a polled request and prevents a racing timer from scheduling more work.
func TestPollingClientClose(t *testing.T) {
	for _, closeHandle := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "active subscription", true: "released subscription"}[closeHandle],
			func(t *testing.T) {
				client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
				Set(client, "task", 1)
				started, canceled := make(chan struct{}), make(chan struct{})
				query := Query(client, "task", func(ctx context.Context) (int, error) {
					close(started)
					<-ctx.Done()
					close(canceled)
					return 0, ctx.Err()
				}, QueryOptions{RefetchInterval: time.Second})
				t.Cleanup(query.Close)
				nextPollingTimer(t, timers).fire()
				<-started
				current := nextPollingTimer(t, timers)
				if closeHandle {
					query.Close()
				}
				select {
				case <-canceled:
					t.Fatal("closing a subscriber canceled a client-owned request")
				default:
				}
				client.Close()
				<-canceled
				current.fire()
				assertNoPollingTimer(t, timers)
				if !current.stopped.Load() || client.Stats().Loads != 1 {
					t.Fatal("closed client retained scheduled polling")
				}
			},
		)
	}
}

// TestPollingIndependentKeys keeps each key's interval and timer ownership independent.
func TestPollingIndependentKeys(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	Set(client, "first", 0)
	Set(client, "second", 0)
	first := Query(client, "first", func(context.Context) (int, error) { return 1, nil },
		QueryOptions{RefetchInterval: time.Second})
	t.Cleanup(first.Close)
	firstTimer := nextPollingTimer(t, timers)
	second := Query(client, "second", func(context.Context) (int, error) { return 2, nil },
		QueryOptions{RefetchInterval: 2 * time.Second})
	t.Cleanup(second.Close)
	secondTimer := nextPollingTimer(t, timers)
	if firstTimer.stopped.Load() || secondTimer.delay != 2*time.Second {
		t.Fatal("another key replaced the first key's schedule")
	}
	secondTimer.fire()
	awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	current := nextPollingTimer(t, timers)
	first.Close()
	if current.stopped.Load() || !firstTimer.stopped.Load() {
		t.Fatal("closing one key affected the other key's timer")
	}
	client.Close()
	if !current.stopped.Load() {
		t.Fatal("client closure did not stop every key's timer")
	}
	firstTimer.fire()
	current.fire()
	assertNoPollingTimer(t, timers)
}
