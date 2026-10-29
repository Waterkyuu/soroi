package soroi

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"time"
)

// ErrQueryBusy indicates a page append conflicts with an initial load or whole-sequence refresh.
var ErrQueryBusy = errors.New("query is already running a different operation")

// InfiniteData contains immutable pages and the parameters used to fetch them.
// Pages and PageParams must have equal lengths and corresponding input order.
type InfiniteData[Page, Param any] struct {
	// Pages contains successfully loaded pages in order.
	Pages []Page
	// PageParams corresponds one-to-one with Pages, including the initial page parameter.
	PageParams []Param
}

// PageContext supplies a continuation callback with matching page and request parameter history.
// Page is one response type; Param is its request parameter type. All values are shared and read-only.
// History includes the last page; during refresh, it contains only the successfully reloaded prefix.
type PageContext[Page, Param any] struct {
	// LastPage is the last successfully loaded response in AllPages.
	LastPage Page
	// AllPages contains successfully loaded responses in request order, including LastPage.
	AllPages []Page
	// LastPageParam is the request parameter used to load LastPage.
	LastPageParam Param
	// AllPageParams corresponds one-to-one with AllPages, including LastPageParam.
	AllPageParams []Param
}

// InfiniteSnapshot adds forward pagination state to an ordinary query snapshot.
type InfiniteSnapshot[Page, Param any] struct {
	// Snapshot provides data, errors, freshness, and shared request activity.
	Snapshot[InfiniteData[Page, Param]]
	// HasNextPage reports whether the last loaded page provides a continuation.
	HasNextPage bool
	// FetchingNextPage distinguishes an append from initial loading and whole-sequence refreshes.
	FetchingNextPage bool
}

// pageSequence composes independent page attempts under one shared query operation and timeout.
type pageSequence func(context.Context, any, bool, func(Loader[any]) (any, error)) (any, error)

// infinitePagination captures the application-owned page loader and continuation policy.
type infinitePagination[Page, Param any] struct {
	// initial starts both initial loading and whole-sequence refreshes.
	initial Param
	// fetch loads one page under the ordinary query retry and execution policy.
	fetch func(context.Context, Param) (Page, error)
	// next extracts a continuation without performing I/O or modifying cached values.
	next func(PageContext[Page, Param]) (Param, bool)
}

// InfiniteQueryHandle owns a subscription to shared pages and their pagination state.
// As with QueryHandle, Close releases this subscription without canceling shared work.
type InfiniteQueryHandle[Page, Param any] struct {
	// query owns registration, loading policy, and the existing handle lifecycle operations.
	query *QueryHandle[InfiniteData[Page, Param]]
	// pagination retains the immutable page-loading configuration for this consumer.
	pagination *infinitePagination[Page, Param]
	// updates carries projected latest states; one worker owns delivery and closure.
	updates chan InfiniteSnapshot[Page, Param]
}

// InfiniteQuery subscribes to pages under one key and automatically loads the first page when enabled.
// Page and Param are inferred from initialPageParam, fetchPage, and getNextPageParam.
// Omitted query options inherit Client defaults; extra options fail through Snapshot and Updates.
// Page size and all other result-affecting parameters belong in the key and fetchPage closure.
// getNextPageParam must be a prompt, pure function; it runs outside Client.mu and may run concurrently.
// The callback receives a PageContext containing the last page and parameter with their matching history.
// During refresh, its history contains only the successfully reloaded prefix, including the latest page.
// All callback inputs are shared, read-only data, including their slices and nested values.
// A false continuation ends pagination, independently of page length or the parameter's zero value.
// Nil getNextPageParam panics before admission; nil fetchPage reports ErrNoFetcher.
func InfiniteQuery[Page, Param any](
	client *Client,
	key any,
	initialPageParam Param,
	fetchPage func(context.Context, Param) (Page, error),
	getNextPageParam func(PageContext[Page, Param]) (Param, bool),
	options ...QueryOptions,
) *InfiniteQueryHandle[Page, Param] {
	if getNextPageParam == nil {
		panic("soroi: nil next page function")
	}
	option, err := singleOptions(options)
	pagination := &infinitePagination[Page, Param]{initial: initialPageParam, fetch: fetchPage, next: getNextPageParam}
	request := queryRequest{options: resolvePolicy(client.options, option), ordinary: true, trigger: TriggerQuery}
	if fetchPage != nil {
		request.load = pagination.loader(initialPageParam)
		request.pages = pagination.load
	}
	query := &QueryHandle[InfiniteData[Page, Param]]{client: client, key: key, err: err}
	h := &InfiniteQueryHandle[Page, Param]{
		query: query, pagination: pagination, updates: make(chan InfiniteSnapshot[Page, Param], 1),
	}
	raw := make(chan InfiniteSnapshot[Page, Param], 1)
	client.mu.Lock()
	if query.err == nil {
		query.observer = &subscription{
			publish: func(state Snapshot[any]) {
				publishLatest(raw, h.snapshotLocked(state))
			},
			close: func() { close(raw) },
		}
		query.err = client.subscribeLocked(querySpec{
			key: key, typ: reflect.TypeFor[InfiniteData[Page, Param]](), request: request,
			disable: option.Disable, refetchInterval: option.RefetchInterval,
		}, query.observer)
	}
	if query.err != nil {
		raw <- InfiniteSnapshot[Page, Param]{Snapshot: Snapshot[InfiniteData[Page, Param]]{
			Status: Error, Stale: true, Err: query.err,
		}}
		close(raw)
	}
	client.mu.Unlock()
	// Continuation is application code, so it must not execute in a locked subscription callback.
	go func() {
		defer close(h.updates)
		for state := range raw {
			publishLatest(h.updates, h.project(state))
		}
	}()
	return h
}

// Snapshot reads current shared pages without loading and derives continuation outside the Client lock.
func (h *InfiniteQueryHandle[Page, Param]) Snapshot() InfiniteSnapshot[Page, Param] {
	q := h.query
	if q.err != nil {
		return InfiniteSnapshot[Page, Param]{Snapshot: Snapshot[InfiniteData[Page, Param]]{
			Status: Error, Stale: true, Err: q.err,
		}}
	}
	q.client.mu.Lock()
	state := h.snapshotLocked(
		q.client.getSnapshotLocked(q.key, reflect.TypeFor[InfiniteData[Page, Param]](), q.observer.request.options),
	)
	q.client.mu.Unlock()
	return h.project(state)
}

// snapshotLocked captures ordinary state and the shared operation kind at the same instant.
func (h *InfiniteQueryHandle[Page, Param]) snapshotLocked(state Snapshot[any]) InfiniteSnapshot[Page, Param] {
	pending := h.query.client.pending[h.query.key]
	return InfiniteSnapshot[Page, Param]{
		Snapshot:         typedSnapshot[InfiniteData[Page, Param]](state),
		FetchingNextPage: pending != nil && pending.request.nextPage,
	}
}

// project derives continuation from captured data, preserving the publication's operation state.
func (h *InfiniteQueryHandle[Page, Param]) project(state InfiniteSnapshot[Page, Param]) InfiniteSnapshot[Page, Param] {
	if state.HasData && len(state.Data.Pages) > 0 {
		_, state.HasNextPage = h.pagination.nextParam(state.Data)
	}
	return state
}

// Updates returns initial and latest pagination states; slow readers may skip intermediate updates.
// Close releases subscriptions immediately; captured continuation callbacks finish before the channel closes.
func (h *InfiniteQueryHandle[Page, Param]) Updates() <-chan InfiniteSnapshot[Page, Param] {
	return h.updates
}

// FetchNextPage waits for one appended page, or loads the first page when no data exists.
// Concurrent same-key appends share one request. Initial loads and refreshes return ErrQueryBusy.
// An exhausted sequence returns its current data without loading. Errors leave prior cached pages available.
func (h *InfiniteQueryHandle[Page, Param]) FetchNextPage(ctx context.Context) (InfiniteData[Page, Param], error) {
	return h.fetch(ctx, true)
}

// Refetch reloads at most the current page count, deriving each cursor from the newly fetched preceding page.
// Results replace old pages only after complete success; an earlier end shortens the sequence.
// Concurrent refreshes share work; a pending append returns ErrQueryBusy.
func (h *InfiniteQueryHandle[Page, Param]) Refetch(ctx context.Context) (InfiniteData[Page, Param], error) {
	return h.fetch(ctx, false)
}

// fetch admits explicit pagination work through the ordinary shared-request and cancellation pipeline.
func (h *InfiniteQueryHandle[Page, Param]) fetch(ctx context.Context, next bool) (InfiniteData[Page, Param], error) {
	var zero InfiniteData[Page, Param]
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	q := h.query
	q.client.mu.Lock()
	err := q.activeLocked()
	q.client.mu.Unlock()
	if err != nil {
		return zero, err
	}
	if next {
		state := h.Snapshot()
		if !state.Fetching && state.HasData && !state.HasNextPage {
			return state.Data, nil
		}
	}
	request := q.observer.request
	request.nextPage = next
	request.trigger = TriggerRefetch
	if next {
		request.trigger = TriggerFetchNextPage
	}
	value, err := q.client.fetch(ctx, q.key, reflect.TypeFor[InfiniteData[Page, Param]](), request, true, q.observer)
	return typedValue[InfiniteData[Page, Param]](value), err
}

// SetEnabled changes this consumer's automatic loading and polling permission using QueryHandle's rules.
func (h *InfiniteQueryHandle[Page, Param]) SetEnabled(enabled bool) error {
	return h.query.SetEnabled(enabled)
}

// Close releases this subscription and its polling participation without canceling shared work.
func (h *InfiniteQueryHandle[Page, Param]) Close() { h.query.Close() }

// loader erases one page callback while leaving retries and execution capacity to the Client.
func (p *infinitePagination[Page, Param]) loader(param Param) Loader[any] {
	return func(ctx context.Context) (any, time.Time, error) {
		page, err := p.fetch(ctx, param)
		return page, time.Time{}, err
	}
}

// nextParam supplies matching page and parameter history from a non-empty, valid sequence.
func (p *infinitePagination[Page, Param]) nextParam(data InfiniteData[Page, Param]) (Param, bool) {
	last := len(data.Pages) - 1
	return p.next(PageContext[Page, Param]{
		LastPage:      data.Pages[last],
		AllPages:      data.Pages,
		LastPageParam: data.PageParams[last],
		AllPageParams: data.PageParams,
	})
}

// load builds an immutable append or replacement using individually retried page callbacks.
func (p *infinitePagination[Page, Param]) load(
	ctx context.Context,
	previous any,
	next bool,
	run func(Loader[any]) (any, error),
) (any, error) {
	data := typedValue[InfiniteData[Page, Param]](previous)
	if len(data.Pages) != len(data.PageParams) {
		return nil, errors.New("infinite query pages and page params must have equal lengths")
	}
	param := p.initial
	limit := max(1, len(data.Pages))
	result := InfiniteData[Page, Param]{Pages: []Page{}, PageParams: []Param{}}
	if next {
		limit = 1
		if len(data.Pages) > 0 {
			var more bool
			param, more = p.nextParam(data)
			if !more {
				return data, nil
			}
		}
		result.Pages, result.PageParams = slices.Clone(data.Pages), slices.Clone(data.PageParams)
	}
	for i := 0; i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := run(p.loader(param))
		if err != nil {
			return nil, err
		}
		page := typedValue[Page](value)
		result.Pages = append(result.Pages, page)
		result.PageParams = append(result.PageParams, param)
		if i+1 < limit {
			var more bool
			// Refresh continuation must use the rebuilt prefix so changed offsets never reuse old parameters.
			param, more = p.nextParam(result)
			if !more {
				break
			}
		}
	}
	return result, nil
}
