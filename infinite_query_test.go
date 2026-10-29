package soroi

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// infiniteTestPage retains a value and the next cursor independently of page length.
type infiniteTestPage struct {
	// Value identifies the loaded page version.
	Value int
	// Next identifies the next page; zero ends the test sequence.
	Next int
}

// nextInfiniteTestPage extracts continuation without treating an empty page as the end.
func nextInfiniteTestPage(page PageContext[infiniteTestPage, int]) (int, bool) {
	return page.LastPage.Next, page.LastPage.Next != 0
}

// awaitInfiniteState waits for an observable state with a deadlock guard, not a scheduling delay.
func awaitInfiniteState[Page, Param any](
	t *testing.T,
	h *InfiniteQueryHandle[Page, Param],
	matches func(InfiniteSnapshot[Page, Param]) bool,
) InfiniteSnapshot[Page, Param] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		select {
		case state, open := <-h.Updates():
			if !open {
				t.Fatal("subscription closed before expected state")
			}
			if matches(state) {
				return state
			}
		case <-ctx.Done():
			t.Fatal("infinite query did not publish expected state")
		}
	}
}

// TestInfiniteQueryPages reuses cached pages across handles and preserves earlier snapshot slices.
func TestInfiniteQueryPages(t *testing.T) {
	client := newInvalidationClient(t)
	var calls atomic.Int32
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		calls.Add(1)
		return infiniteTestPage{Value: cursor, Next: cursor + 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	if h.Snapshot().HasData || calls.Load() != 0 {
		t.Fatal("disabled query loaded pages")
	}
	first, err := h.FetchNextPage(context.Background())
	if err != nil || len(first.Pages) != 1 || first.PageParams[0] != 0 {
		t.Fatalf("initial page = %+v, %v", first, err)
	}
	other := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage)
	t.Cleanup(other.Close)
	if calls.Load() != 1 || !other.Snapshot().HasNextPage {
		t.Fatal("fresh cached sequence was not reused")
	}
	second, err := other.FetchNextPage(context.Background())
	if err != nil || !reflect.DeepEqual(second.PageParams, []int{0, 1}) {
		t.Fatalf("appended pages = %+v, %v", second, err)
	}
	if len(first.Pages) != 1 || len(first.PageParams) != 1 || first.Pages[0].Value != 0 {
		t.Fatal("append mutated an earlier snapshot")
	}
	state := awaitInfiniteState(t, h, func(s InfiniteSnapshot[infiniteTestPage, int]) bool {
		return len(s.Data.Pages) == 2 && !s.Fetching
	})
	if !state.HasNextPage || state.FetchingNextPage || state.Err != nil || calls.Load() != 2 {
		t.Fatalf("shared append = %+v; calls = %d", state, calls.Load())
	}
}

// TestInfiniteQueryFailureAndRefresh retries one failed page and atomically replaces a refreshed sequence.
func TestInfiniteQueryFailureAndRefresh(t *testing.T) {
	client := newInvalidationClient(t)
	wantErr := errors.New("page unavailable")
	var version, failCursor atomic.Int32
	failCursor.Store(-1)
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		if int(failCursor.Load()) == cursor {
			return infiniteTestPage{}, wantErr
		}
		next := cursor + 1
		if cursor == 2 || version.Load() == 2 {
			next = 0
		}
		return infiniteTestPage{Value: int(version.Load())*10 + cursor, Next: next}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	for range 2 {
		if _, err := h.FetchNextPage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	failCursor.Store(2)
	if _, err := h.FetchNextPage(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("append error = %v", err)
	}
	if state := h.Snapshot(); len(state.Data.Pages) != 2 || !state.HasData || !errors.Is(state.Err, wantErr) {
		t.Fatalf("append failure lost previous pages: %+v", state)
	}
	failCursor.Store(-1)
	if pages, err := h.FetchNextPage(context.Background()); err != nil || len(pages.Pages) != 3 {
		t.Fatalf("retry append = %+v, %v", pages, err)
	}
	version.Store(1)
	failCursor.Store(1)
	if _, err := h.Refetch(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("refresh error = %v", err)
	}
	if state := h.Snapshot(); len(state.Data.Pages) != 3 || state.Data.Pages[0].Value != 0 {
		t.Fatalf("partial refresh replaced old pages: %+v", state)
	}
	failCursor.Store(-1)
	version.Store(2)
	pages, err := h.Refetch(context.Background())
	if err != nil || len(pages.Pages) != 1 || pages.Pages[0].Value != 20 || h.Snapshot().HasNextPage {
		t.Fatalf("shorter refreshed sequence = %+v, %v", pages, err)
	}
	if _, err := h.FetchNextPage(context.Background()); err != nil || len(h.Snapshot().Data.Pages) != 1 {
		t.Fatalf("exhausted sequence = %v", err)
	}
}

// TestInfiniteQueryConcurrentOperations merges duplicate appends and rejects conflicting refreshes.
func TestInfiniteQueryConcurrentOperations(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(ctx context.Context, cursor int) (infiniteTestPage, error) {
		calls.Add(1)
		if cursor == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return infiniteTestPage{}, ctx.Err()
			}
		}
		return infiniteTestPage{Value: cursor, Next: cursor + 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	h.FetchNextPage(context.Background())
	first := make(chan error, 1)
	go func() { _, err := h.FetchNextPage(context.Background()); first <- err }()
	<-started
	if state := h.Snapshot(); !state.Fetching || !state.FetchingNextPage || len(state.Data.Pages) != 1 {
		t.Fatalf("append activity = %+v", state)
	}
	if _, err := h.Refetch(context.Background()); !errors.Is(err, ErrQueryBusy) {
		t.Fatalf("conflicting refresh = %v", err)
	}
	other := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(other.Close)
	if !other.Snapshot().FetchingNextPage {
		t.Fatal("append activity was not shared")
	}
	joined, err := client.SubscribeEvents(EventOptions{Key: "tasks"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(joined.Close)
	second := make(chan error, 1)
	go func() { _, err := other.FetchNextPage(context.Background()); second <- err }()
	event := awaitEventKind(t, joined, EventLoadJoined)
	if event.Trigger != TriggerFetchNextPage || event.Trigger.String() != "fetch_next_page" {
		t.Fatalf("append event = %+v", event)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(h.Snapshot().Data.Pages) != 2 {
		t.Fatal("concurrent append fetched more than one page")
	}
}

// TestInfiniteQueryRetryBudgetAndCallbacks reuses per-page retries and avoids holding a budget across pages.
func TestInfiniteQueryRetryBudgetAndCallbacks(t *testing.T) {
	executor, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(
		Options{StaleTime: time.Hour, Retry: 1, RetryDelay: func(int) time.Duration { return 0 }, Executor: executor},
	)
	t.Cleanup(client.Close)
	var calls [2]atomic.Int32
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		if executor.Stats().Active != 1 {
			t.Error("page callback did not acquire the shared budget")
		}
		if calls[cursor].Add(1) == 1 {
			return infiniteTestPage{}, errors.New("retry")
		}
		return infiniteTestPage{Value: cursor, Next: 1 - cursor}, nil
	}
	next := func(page PageContext[infiniteTestPage, int]) (int, bool) {
		_ = client.Stats()
		return nextInfiniteTestPage(page)
	}
	h := InfiniteQuery(client, "tasks", 0, load, next, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	for range 2 {
		if _, err := h.FetchNextPage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	calls[1].Store(0)
	if _, err := h.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls[0].Load() != 3 || calls[1].Load() != 2 || client.Stats().Retries != 3 {
		t.Fatalf("page retry counts = %d, %d; stats = %+v", calls[0].Load(), calls[1].Load(), client.Stats())
	}
}

// TestInfiniteQuerySupersession prevents detached append results from restoring removed or replaced pages.
func TestInfiniteQuerySupersession(t *testing.T) {
	for _, operation := range []string{"cancel", "remove", "set", "close"} {
		t.Run(operation, func(t *testing.T) {
			client := newInvalidationClient(t)
			started, release := make(chan struct{}), make(chan struct{})
			load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
				if cursor == 1 {
					close(started)
					<-release
				}
				return infiniteTestPage{Value: cursor, Next: cursor + 1}, nil
			}
			h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
			t.Cleanup(h.Close)
			h.FetchNextPage(context.Background())
			done := make(chan error, 1)
			go func() { _, err := h.FetchNextPage(context.Background()); done <- err }()
			<-started
			switch operation {
			case "cancel":
				client.Cancel("tasks")
			case "remove":
				client.Remove("tasks")
			case "set":
				Set(
					client,
					"tasks",
					InfiniteData[infiniteTestPage, int]{Pages: []infiniteTestPage{{Value: 99}}, PageParams: []int{0}},
				)
			case "close":
				client.Close()
			}
			close(release)
			<-done
			state := h.Snapshot()
			switch operation {
			case "cancel":
				if len(state.Data.Pages) != 1 {
					t.Fatal("cancellation lost old data or applied detached append")
				}
			case "remove", "close":
				if state.HasData {
					t.Fatal("detached append restored removed pages")
				}
			case "set":
				if len(state.Data.Pages) != 1 || state.Data.Pages[0].Value != 99 {
					t.Fatal("detached append overwrote local data")
				}
			}
		})
	}
}

// TestInfiniteQueryPollingAndInvalidation shares existing timers, enablement, and awaited refresh receipts.
func TestInfiniteQueryPollingAndInvalidation(t *testing.T) {
	client, timers := newPollingClient(t, Options{StaleTime: time.Hour})
	var calls atomic.Int32
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		return infiniteTestPage{Value: int(calls.Add(1)), Next: cursor + 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{RefetchInterval: time.Second})
	t.Cleanup(h.Close)
	awaitInfiniteState(t, h, func(s InfiniteSnapshot[infiniteTestPage, int]) bool { return s.HasData && !s.Fetching })
	h.FetchNextPage(context.Background())
	first := nextPollingTimer(t, timers)
	first.fire()
	awaitInfiniteState(t, h, func(s InfiniteSnapshot[infiniteTestPage, int]) bool {
		return len(s.Data.Pages) == 2 && s.Data.Pages[0].Value == 3 && !s.Fetching
	})
	if results, err := client.InvalidateAndWait(context.Background(), "tasks"); err != nil || !results[0].Refreshed {
		t.Fatalf("awaited refresh = %+v, %v", results, err)
	}
	if calls.Load() != 6 {
		t.Fatalf("whole-sequence refresh calls = %d", calls.Load())
	}
	current := nextPollingTimer(t, timers)
	if err := h.SetEnabled(false); err != nil {
		t.Fatal(err)
	}
	current.fire()
	if !current.stopped.Load() || calls.Load() != 6 {
		t.Fatal("disabled query continued polling")
	}
	h.Close()
	for range h.Updates() {
	}
	if _, err := h.FetchNextPage(context.Background()); !errors.Is(err, ErrQueryClosed) {
		t.Fatalf("released handle append = %v", err)
	}
}

// TestInfiniteQueryRefreshUsesNewCursors follows a changed cursor chain instead of reusing old page parameters.
func TestInfiniteQueryRefreshUsesNewCursors(t *testing.T) {
	client := newInvalidationClient(t)
	var changed atomic.Bool
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		next := 1
		if changed.Load() {
			next = 9
		}
		if cursor != 0 {
			next = 0
		}
		return infiniteTestPage{Value: cursor, Next: next}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	for range 2 {
		if _, err := h.FetchNextPage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	changed.Store(true)
	pages, err := h.Refetch(context.Background())
	if err != nil || !reflect.DeepEqual(pages.PageParams, []int{0, 9}) || pages.Pages[1].Value != 9 {
		t.Fatalf("refreshed cursors = %+v, %v", pages, err)
	}
}

// TestInfiniteQueryRefreshBusy reports conflicts without changing the pending refresh or retained data.
func TestInfiniteQueryRefreshBusy(t *testing.T) {
	client := newInvalidationClient(t)
	started := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	load := func(ctx context.Context, cursor int) (infiniteTestPage, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return infiniteTestPage{}, ctx.Err()
		}
		return infiniteTestPage{Value: cursor, Next: 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage)
	t.Cleanup(h.Close)
	<-started
	if state := h.Snapshot(); state.Status != Pending || !state.Fetching || state.FetchingNextPage {
		t.Fatalf("initial state = %+v", state)
	}
	if _, err := h.FetchNextPage(context.Background()); !errors.Is(err, ErrQueryBusy) {
		t.Fatalf("append during initial load = %v", err)
	}
	release <- struct{}{}
	awaitInfiniteState(t, h, func(s InfiniteSnapshot[infiniteTestPage, int]) bool { return s.HasData && !s.Fetching })
	done := make(chan error, 1)
	go func() { _, err := h.Refetch(context.Background()); done <- err }()
	<-started
	if _, err := h.FetchNextPage(context.Background()); !errors.Is(err, ErrQueryBusy) {
		t.Fatalf("append during refresh = %v", err)
	}
	if state := h.Snapshot(); state.FetchingNextPage || !state.HasData || !state.Fetching {
		t.Fatalf("refresh state = %+v", state)
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestInfiniteQueryAgeAndLocalUpdates preserves ordinary cache operations and restarts expired sequences.
func TestInfiniteQueryAgeAndLocalUpdates(t *testing.T) {
	now := time.Unix(0, 0)
	client := NewClient(Options{StaleTime: time.Hour, MaxAge: time.Minute, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	load := func(_ context.Context, cursor int) (infiniteTestPage, error) {
		return infiniteTestPage{Value: cursor, Next: cursor + 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	for range 2 {
		h.FetchNextPage(context.Background())
	}
	if err := Update(
		client,
		"tasks",
		func(data InfiniteData[infiniteTestPage, int]) (InfiniteData[infiniteTestPage, int], error) {
			data.Pages = append([]infiniteTestPage{}, data.Pages...)
			data.Pages[0].Value = 42
			return data, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if Get[InfiniteData[infiniteTestPage, int]](client, "tasks").Data.Pages[0].Value != 42 {
		t.Fatal("ordinary cache operations did not update the sequence")
	}
	now = now.Add(time.Minute)
	if state := h.Snapshot(); state.HasData || state.HasNextPage {
		t.Fatalf("expired pagination = %+v", state)
	}
	if data, err := h.FetchNextPage(
		context.Background(),
	); err != nil || len(data.Pages) != 1 ||
		data.PageParams[0] != 0 {
		t.Fatalf("expired sequence restart = %+v, %v", data, err)
	}
}

// TestInfiniteQueryValidation reports existing construction errors without admitting loaders.
func TestInfiniteQueryValidation(t *testing.T) {
	for _, name := range []string{"key", "type", "fetcher", "options", "closed"} {
		t.Run(name, func(t *testing.T) {
			client := newInvalidationClient(t)
			var calls atomic.Int32
			var key any = "tasks"
			load := func(context.Context, int) (infiniteTestPage, error) {
				calls.Add(1)
				return infiniteTestPage{}, nil
			}
			options := []QueryOptions{}
			var expected error
			switch name {
			case "key":
				key, expected = []string{"tasks"}, ErrInvalidKey
			case "type":
				Set(client, key, 42)
				expected = ErrTypeMismatch
			case "fetcher":
				load, expected = nil, ErrNoFetcher
			case "options":
				options, expected = []QueryOptions{{}, {}}, ErrTooManyOptions
			case "closed":
				client.Close()
				expected = ErrClosed
			}
			h := InfiniteQuery(client, key, 0, load, nextInfiniteTestPage, options...)
			t.Cleanup(h.Close)
			if !errors.Is(h.Snapshot().Err, expected) || calls.Load() != 0 {
				t.Fatalf("construction = %+v; calls = %d", h.Snapshot(), calls.Load())
			}
			if _, err := h.FetchNextPage(context.Background()); !errors.Is(err, expected) {
				t.Fatalf("failed handle append = %v", err)
			}
			if _, err := h.Refetch(context.Background()); !errors.Is(err, expected) {
				t.Fatalf("failed handle refresh = %v", err)
			}
			if err := h.SetEnabled(true); !errors.Is(err, expected) {
				t.Fatalf("failed handle enablement = %v", err)
			}
			count := 0
			for state := range h.Updates() {
				count++
				if !errors.Is(state.Err, expected) {
					t.Fatalf("failed handle update = %+v", state)
				}
			}
			if count != 1 {
				t.Fatalf("construction error updates = %d", count)
			}
		})
	}
}

// TestInfiniteQueryParameterTypes infers string cursors and accepts a zero-valued continuation explicitly.
func TestInfiniteQueryParameterTypes(t *testing.T) {
	client := newInvalidationClient(t)
	load := func(_ context.Context, cursor string) (string, error) { return cursor, nil }
	h := InfiniteQuery(
		client,
		"tasks",
		"first",
		load,
		func(page PageContext[string, string]) (string, bool) {
			return "", page.LastPage == "first"
		},
		QueryOptions{Disable: true},
	)
	t.Cleanup(h.Close)
	for range 2 {
		if _, err := h.FetchNextPage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if state := h.Snapshot(); !reflect.DeepEqual(state.Data.Pages, []string{"first", ""}) || state.HasNextPage {
		t.Fatalf("zero-valued continuation = %+v", state)
	}
}

// TestInfiniteQueryInvalidationDuringAppend waits for a bounded full refresh after the append finishes.
func TestInfiniteQueryInvalidationDuringAppend(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(ctx context.Context, cursor int) (infiniteTestPage, error) {
		call := calls.Add(1)
		if call == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return infiniteTestPage{}, ctx.Err()
			}
		}
		return infiniteTestPage{Value: int(call), Next: cursor + 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	h.FetchNextPage(context.Background())
	if err := h.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	appended := make(chan error, 1)
	go func() { _, err := h.FetchNextPage(context.Background()); appended <- err }()
	<-started
	events, err := client.SubscribeEvents(EventOptions{Key: "tasks"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(events.Close)
	refreshed := make(chan error, 1)
	go func() { _, err := client.InvalidateAndWait(context.Background(), "tasks"); refreshed <- err }()
	awaitEventKind(t, events, EventInvalidated)
	close(release)
	if err := <-appended; err != nil {
		t.Fatal(err)
	}
	if err := <-refreshed; err != nil {
		t.Fatal(err)
	}
	if state := h.Snapshot(); len(state.Data.Pages) != 2 || state.Data.Pages[0].Value != 3 || state.Stale ||
		calls.Load() != 4 {
		t.Fatalf("follow-up refresh = %+v; calls = %d", state, calls.Load())
	}
}

// TestInfiniteQueryCallerCancellation stops an owned page request without discarding earlier successful pages.
func TestInfiniteQueryCallerCancellation(t *testing.T) {
	client := newInvalidationClient(t)
	started := make(chan struct{})
	load := func(ctx context.Context, cursor int) (infiniteTestPage, error) {
		if cursor == 1 {
			close(started)
			<-ctx.Done()
			return infiniteTestPage{}, ctx.Err()
		}
		return infiniteTestPage{Next: 1}, nil
	}
	h := InfiniteQuery(client, "tasks", 0, load, nextInfiniteTestPage, QueryOptions{Disable: true})
	t.Cleanup(h.Close)
	h.FetchNextPage(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := h.FetchNextPage(ctx); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("owned append cancellation = %v", err)
	}
	state := awaitInfiniteState(t, h, func(s InfiniteSnapshot[infiniteTestPage, int]) bool { return !s.Fetching })
	if !state.HasData || len(state.Data.Pages) != 1 || state.FetchingNextPage {
		t.Fatalf("canceled append = %+v", state)
	}
}

// TestInfiniteQueryNilContinuation rejects a missing continuation before any subscription is registered.
func TestInfiniteQueryNilContinuation(t *testing.T) {
	client := newInvalidationClient(t)
	defer func() {
		if recover() == nil {
			t.Error("nil continuation did not panic")
		}
		if len(client.observers) != 0 || client.Stats().Loads != 0 {
			t.Error("nil continuation admitted work")
		}
	}()
	InfiniteQuery(client, "tasks", 0, func(context.Context, int) (int, error) { return 1, nil }, nil)
}

// TestInfiniteQueryContinuationHistory supplies matching history during append, projection, and sequential refresh.
func TestInfiniteQueryContinuationHistory(t *testing.T) {
	client := newInvalidationClient(t)
	var generation atomic.Int32
	var fail atomic.Bool
	wantErr := errors.New("page unavailable")
	load := func(_ context.Context, offset int) ([]int, error) {
		if fail.Load() {
			return nil, wantErr
		}
		version := int(generation.Load())
		page := make([]int, 2+version)
		for i := range page {
			page[i] = version*100 + offset + i
		}
		return page, nil
	}
	next := func(page PageContext[[]int, int]) (int, bool) {
		last, pages := page.LastPage, page.AllPages
		lastOffset, offsets := page.LastPageParam, page.AllPageParams
		if len(pages) == 0 || len(pages) != len(offsets) {
			t.Errorf("continuation history = %v, %v", pages, offsets)
			return 0, false
		}
		end := len(pages) - 1
		if !reflect.DeepEqual(last, pages[end]) || lastOffset != offsets[end] {
			t.Errorf("continuation tail = %v, %d; history = %v, %v", last, lastOffset, pages, offsets)
		}
		expectedOffset := 0
		for i, page := range pages {
			if offsets[i] != expectedOffset {
				t.Errorf("page %d offset = %d; want %d", i, offsets[i], expectedOffset)
			}
			for j, item := range page {
				if item != pages[0][0]+offsets[i]+j {
					t.Errorf("page %d mixes old and refreshed history: %v", i, pages)
				}
			}
			expectedOffset += len(page)
		}
		return lastOffset + len(last), lastOffset+len(last) < 6
	}
	h := InfiniteQuery(client, "tasks", 0, load, next, QueryOptions{Disable: true})
	defer func() {
		h.Close()
		for range h.Updates() {
		}
	}()
	for range 3 {
		if _, err := h.FetchNextPage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if state := h.Snapshot(); state.HasNextPage || !reflect.DeepEqual(state.Data.PageParams, []int{0, 2, 4}) {
		t.Fatalf("offset sequence = %+v", state)
	}
	state := awaitInfiniteState(t, h, func(s InfiniteSnapshot[[]int, int]) bool {
		return s.HasData && !s.Fetching && len(s.Data.Pages) == 3
	})
	if state.HasNextPage {
		t.Error("exhausted update reports another page")
	}
	generation.Store(1)
	fail.Store(true)
	if _, err := h.Refetch(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("failed offset refresh = %v", err)
	}
	if state := h.Snapshot(); !reflect.DeepEqual(state.Data.PageParams, []int{0, 2, 4}) {
		t.Fatalf("failed refresh replaced history = %+v", state)
	}
	fail.Store(false)
	data, err := h.Refetch(context.Background())
	if err != nil || !reflect.DeepEqual(data.PageParams, []int{0, 3}) {
		t.Fatalf("refreshed offset sequence = %+v, %v", data, err)
	}
	if state := h.Snapshot(); state.HasNextPage || len(state.Data.Pages) != 2 {
		t.Fatalf("refreshed offset state = %+v", state)
	}
}
