// Package soroi provides typed shared query state, caching, retries, and background refreshes.
package soroi

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Client owns shared results of different types and merges concurrent requests by key.
// Reuse one client across components and call Close to stop background work.
type Client struct {
	// mu coordinates operations that require a coherent view of every bucket.
	mu clientMutex
	// seed fixes routing for this Client lifetime, including after Clear and Close.
	seed maphash.Seed
	// buckets owns stable key partitions; it is never resized or replaced.
	buckets []clientBucket
	// groups indexes live grouped subscriptions while every bucket lock is held.
	groups map[*queriesGroup]struct{}
	// concurrentClock permits parallel reads only when the client owns its concurrency-safe default clock.
	concurrentClock bool
	// eventSubscriptions indexes diagnostic streams by exact key; nil owns all-key subscriptions under mu.
	eventSubscriptions map[any]map[*EventSubscription]struct{}
	// eventMu serializes ordered event emission across independent key owners.
	eventMu sync.Mutex
	// eventCount permits subscription-free reads without accessing the subscription index.
	eventCount atomic.Int64
	// eventSequence orders emitted diagnostic records without retaining history.
	eventSequence uint64
	// nextLoadID assigns operation identities even without active diagnostic subscriptions.
	nextLoadID atomic.Uint64
	// loadNow measures elapsed load time independently of the freshness clock; tests may replace it.
	loadNow func() time.Time
	// nextRevision distinguishes writes even when a key is removed and recreated.
	nextRevision atomic.Uint64
	// recency orders retained results and errors from least to most recently used.
	recency list.List
	// recencyEntries connects cached keys to their positions without scanning the cache.
	recencyEntries map[any]*list.Element
	// nextObserver orders subscriptions so automatic refresh chooses the oldest enabled consumer.
	nextObserver atomic.Uint64
	// gcAfterFunc schedules cleanup and returns a stop function; tests can replace the timer source.
	gcAfterFunc func(time.Duration, func()) func()
	// pollAfterFunc schedules interval callbacks and returns their stop functions; tests replace the timer source.
	pollAfterFunc func(time.Duration, func()) func()
	// options supplies freshness and request policies.
	options Options
	// ctx owns background loads until Close is called.
	ctx context.Context
	// cancel stops the client's background lifetime.
	cancel context.CancelFunc
	// closed prevents new work after resource release.
	closed bool
}

// entry retains one typed result, including stale data and the latest load error.
type entry struct {
	// tags retains the union of labels registered for this key until its query state is released.
	tags []string
	// revision changes when data or its invalidation state is replaced.
	revision uint64
	// typ binds this key to one static result type until its state and subscriptions are released.
	typ reflect.Type
	// value is shared, immutable query data.
	value any
	// hasData distinguishes a usable result from an initial failure.
	hasData bool
	// expiresAt controls reuse independently of how long data stays available.
	expiresAt time.Time
	// ordinary allows consumers to evaluate freshness independently for data without an explicit deadline.
	ordinary bool
	// invalidated forces ordinary data stale independently of consumer freshness after invalidation or failure.
	invalidated bool
	// updatedAt records the last installation of usable data.
	updatedAt time.Time
	// fetchedAt preserves source freshness when sharedExpiresAt identifies a shared result.
	fetchedAt time.Time
	// sharedExpiresAt caps every consumer's freshness for a shared result; zero means no shared cap.
	sharedExpiresAt time.Time
	// shared retains remote notification identity independently of data freshness and local writes.
	shared sharedBinding
	// err records the most recent failure while earlier data can remain available.
	err error
	// staleReason records the latest explicit stale cause without keeping removed-key history.
	staleReason EventReason
}

// queryRequest owns an erased loader and the initiating consumer's copied request policy.
type queryRequest struct {
	// pages optionally composes page loads through the existing retry and execution pipeline.
	pages pageSequence
	// nextPage selects an append instead of refreshing an entire page sequence.
	nextPage bool
	// tags owns copied labels supplied by a definition; admission merges them into the shared entry.
	tags []string
	// load produces data and an optional explicit expiration outside the client lock.
	load Loader[any]
	// fetcher retains an ordinary typed function without allocating a per-read wrapper.
	fetcher ordinaryFetcher
	// shared optionally performs typed shared-cache access within the existing loader attempt.
	shared *sharedQuery
	// options supplies the initiator's retry, timeout, and ordinary freshness settings.
	options Options
	// ordinary distinguishes fetchers from loaders with authoritative absolute deadlines.
	ordinary bool
	// trigger identifies the API decision initiating or joining this request.
	trigger EventTrigger
}

// flight shares one loader's completion with all waiting callers.
type flight struct {
	// previous captures immutable data before a page sequence starts loading.
	previous any
	// id identifies this shared operation within its Client.
	id uint64
	// reason describes the initiating consumer's stale decision or forced refresh.
	reason EventReason
	// joined counts additional joins independently of current waiter count.
	joined uint64
	// attempts counts actual loader invocations under the Client lock.
	attempts int
	// discardReason identifies an explicit operation that detached this load.
	discardReason EventReason
	// request preserves the initiator's policy while other consumers join the load.
	request queryRequest
	// startedAt marks the beginning of shared work, including scheduling and retry waits.
	startedAt time.Time
	// done closes after value and err have been published.
	done chan struct{}
	// cancel stops this load without canceling unrelated query keys.
	cancel context.CancelFunc
	// owner distinguishes caller cancellation from the client's configured timeout.
	owner context.Context
	// detached prevents explicit Cancel, Remove, and Set from restarting the canceled load.
	detached bool
	// value becomes visible when done closes.
	value any
	// err becomes visible when done closes.
	err error
	// invalidated prevents a refresh invalidated in flight from being marked fresh.
	invalidated bool
	// refresh reports the outcome to callers waiting for this invalidation-triggered load.
	refresh *refreshCompletion
	// followup coalesces invalidations requiring a load after this one finishes.
	followup *refreshCompletion
	// followupLoad prevents a chain of automatic follow-ups from growing without bound.
	followupLoad bool
}

// NewClient constructs an independently owned client with optional freshness and retry policies.
func NewClient(options Options) *Client {
	return newClient(options, defaultBucketCount)
}

// newClient constructs one lifetime and a fixed power-of-two set of storage buckets.
// The internal count supports parameter experiments without adding public configuration.
func newClient(options Options, bucketCount int) *Client {
	if bucketCount <= 0 || bucketCount&(bucketCount-1) != 0 {
		panic("bucket count must be a positive power of two")
	}
	concurrentClock := options.Clock == nil
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RetryDelay == nil {
		options.RetryDelay = retryDelay
	}
	// The cancellation is transferred to Client.Close rather than a constructor defer.
	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- Close owns the client's lifetime.
	client := &Client{
		concurrentClock: concurrentClock,
		options:         options, ctx: ctx, cancel: cancel,
		loadNow: time.Now, gcAfterFunc: scheduleGC, pollAfterFunc: schedulePoll,
		seed: maphash.MakeSeed(), buckets: make([]clientBucket, bucketCount),
	}
	client.recencyEntries = make(map[any]*list.Element)
	client.mu.owner = client
	// Nil per-bucket maps are intentional: only a first write allocates each optional index.
	for i := range client.buckets {
		bucket := &client.buckets[i]
		bucket.index = i
		bucket.owner = client
		bucket.mu.bucket = bucket
	}
	return client
}

// Invalidate marks targets selected by an exact key, []any, func(any) bool, or QueryFilter stale.
// Completed data is retained while selected queries become stale.
// The zero-value configuration refreshes enabled observed queries only when no load is active.
// RefetchAfterLoad optionally coalesces in-flight invalidations into one bounded follow-up.
// Omitted options use zero values; extras return ErrTooManyOptions before selecting or changing targets.
// Batches are validated before changes; predicates run outside the lock against existing keys.
// Filter and predicate selection use existing keys only, exclude additions, and skip removed keys.
// Nil targets or direct predicates are invalid; a nil QueryFilter.Predicate imposes no restriction.
// Empty batches succeed; closed clients return ErrClosed without evaluating predicates.
func (c *Client) Invalidate(target any, options ...InvalidateOptions) error {
	option, err := singleOptions(options)
	if err != nil {
		return err
	}
	_, err = c.invalidate(target, option, nil)
	return err
}

// invalidate applies shared target selection and records the scheduled refresh for each selected key.
func (c *Client) invalidate(
	target any,
	options InvalidateOptions,
	waitCtx context.Context,
) ([]*refreshCompletion, error) {
	refreshes := make([]*refreshCompletion, 0)
	err := c.withQueryTargets(target, func(bucket *clientBucket, key any) {
		refresh := bucket.invalidateLocked(key, options, waitCtx)
		if waitCtx != nil {
			refreshes = append(refreshes, refresh)
		}
	})
	if err != nil {
		return nil, err
	}
	return refreshes, nil
}

// invalidateLocked marks stale state and captures the exact refresh required by this invalidation.
func (c *clientBucket) invalidateLocked(
	key any,
	options InvalidateOptions,
	waitCtx context.Context,
) *refreshCompletion {
	cached := c.entryLocked(key)
	cached.expiresAt = time.Time{}
	cached.invalidated = true
	cached.staleReason = ReasonInvalidated
	c.advanceRevisionLocked(&cached)
	c.storeEntryLocked(key, cached)
	event := Event{Key: key, Kind: EventInvalidated, Trigger: TriggerInvalidate, Reason: ReasonInvalidated}
	pending := c.pending[key]
	if pending != nil {
		pending.invalidated = true
		event.LoadID = pending.id
	}
	c.owner.emitEventLocked(event)
	refresh := c.scheduleRefreshLocked(key, pending, options, waitCtx)
	if refresh == nil && waitCtx != nil {
		refresh = newRefreshCompletion(key)
		refresh.finish(false, nil)
	}
	c.touchCapacityLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
	return refresh
}

// Cancel stops selected active loads while retaining previously completed data and subscriptions.
// Targets accept an exact key, []any, func(any) bool, or QueryFilter with the same rules as Invalidate.
func (c *Client) Cancel(target any) error {
	return c.withQueryTargets(target, func(bucket *clientBucket, key any) {
		bucket.cancelLocked(key, ReasonCancel)
		bucket.touchGCLocked(key)
		bucket.notifyLocked(key)
	})
}

// Remove discards selected results and prevents their active loaders from restoring removed data.
// Targets accept an exact key, []any, func(any) bool, or QueryFilter; live subscriptions remain usable.
func (c *Client) Remove(target any) error {
	return c.withQueryTargets(target, func(bucket *clientBucket, key any) {
		bucket.cancelLocked(key, ReasonRemove)
		bucket.discardLocked(key, ReasonRemove)
		bucket.stopGCLocked(key)
		bucket.notifyLocked(key)
	})
}

// Clear removes all results and cancels active loads while retaining live subscriptions.
func (c *Client) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forEachBucketLocked(func(bucket *clientBucket) {
		for key := range bucket.pending {
			bucket.cancelLocked(key, ReasonClear)
		}
		for key := range bucket.entries {
			bucket.discardLocked(key, ReasonClear)
		}
		bucket.clearGCLocked()
		for key := range bucket.observers {
			bucket.notifyLocked(key)
		}
	})
}

// Close cancels active work, closes subscriptions, and prevents new requests; it is idempotent.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.cancel()
	c.forEachBucketLocked(func(bucket *clientBucket) {
		for key := range bucket.pollTasks {
			bucket.stopPollingLocked(key)
		}
		for key := range bucket.pending {
			bucket.cancelLocked(key, ReasonClientClosed)
		}
		for _, observers := range bucket.observers {
			for observer := range observers {
				observer.close()
			}
		}
		clear(bucket.observers)
		clear(bucket.entries)
		bucket.clearGCLocked()
	})
	c.recency.Init()
	clear(c.recencyEntries)
	c.emitEventLocked(Event{Kind: EventClientClosed, Reason: ReasonClientClosed})
	for _, subscriptions := range c.eventSubscriptions {
		for subscription := range subscriptions {
			close(subscription.events)
		}
	}
	clear(c.eventSubscriptions)
	c.eventCount.Store(0)
}

// startLocked publishes pending state before starting one asynchronous load.
func (c *clientBucket) startLocked(ctx context.Context, key any, request queryRequest) *flight {
	c.stopGCLocked(key)
	var previous any
	if request.pages != nil {
		c.expireDataLocked(key)
		previous = c.entryLocked(key).value
		// Missing or expired pages make FetchNextPage an initial load, rather than an append.
		if !c.entryLocked(key).hasData {
			request.nextPage = false
		}
	}
	owner := ctx
	ctx, cancel := context.WithCancel(ctx)
	if request.options.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, request.options.Timeout)
		previousCancel := cancel
		cancel = func() { timeoutCancel(); previousCancel() }
	}
	pending := &flight{
		previous:  previous,
		id:        c.owner.nextLoadIdentity(),
		reason:    loadReason(c.entryLocked(key)),
		done:      make(chan struct{}),
		cancel:    cancel,
		owner:     owner,
		startedAt: c.owner.loadNow(),
		request:   request,
	}
	if request.trigger == TriggerPoll {
		pending.reason = ReasonRefetch
	}
	if c.pending == nil {
		c.pending = make(map[any]*flight)
	}
	c.pending[key] = pending
	c.stats.Loads++
	c.owner.emitEventLocked(pending.event(key, EventLoadStarted))
	c.notifyLocked(key)
	go c.execute(ctx, key, pending)
	return pending
}

// execute publishes a completed load only while it still owns the key's active request.
func (c *clientBucket) execute(ctx context.Context, key any, pending *flight) {
	defer pending.cancel()
	value, expiresAt, err := c.run(ctx, key, pending)
	var source sharedResult
	if result, ok := value.(sharedResult); ok {
		source = result
		value = result.value
	}
	if ctx.Err() != nil {
		value, expiresAt, err = nil, time.Time{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	duration := c.owner.loadNow().Sub(pending.startedAt)
	// A changed remote generation rejects persistence and also prevents this older snapshot becoming locally fresh.
	pending.invalidated = pending.invalidated || source.invalidated
	c.stats.TotalLoadDuration += duration
	switch {
	case err == nil:
		c.stats.LoadSuccesses++
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		c.stats.LoadCancellations++
	default:
		c.stats.LoadFailures++
	}
	var applied bool
	if c.pending[key] == pending {
		delete(c.pending, key)
		if !c.owner.closed && ctx.Err() == nil {
			c.expireDataLocked(key)
			cached := c.entryLocked(key)
			installsData := err == nil || c.owner.options.Clock().Before(expiresAt)
			if installsData {
				cached.value, cached.hasData, cached.updatedAt = value, true, c.owner.options.Clock()
				cached.fetchedAt, cached.sharedExpiresAt = source.fetchedAt, source.expiresAt
				for _, tag := range source.tags {
					if !matchesQueryTags(cached.shared.tags, []string{tag}) {
						cached.shared.tags = append(cached.shared.tags, tag)
					}
				}
				cached.ordinary = pending.request.ordinary
				if cached.ordinary {
					expiresAt = c.ordinaryExpiry(cached, pending.request.options.StaleTime)
				}
				expiresAt = c.limitExpiry(cached.updatedAt, expiresAt)
			}
			if pending.invalidated {
				expiresAt = time.Time{}
			}
			cached.invalidated = pending.invalidated || err != nil
			cached.staleReason = ReasonNone
			if pending.invalidated {
				cached.staleReason = ReasonInvalidated
			} else if err != nil && !installsData {
				cached.staleReason = ReasonPreviousFailure
			}
			cached.err, cached.expiresAt = err, expiresAt
			c.advanceRevisionLocked(&cached)
			c.storeEntryLocked(key, cached)
			applied = true
			c.touchCapacityLocked(key)
		}
		c.touchGCLocked(key)
	}
	pending.value, pending.err = value, err
	event := pending.event(key, EventLoadFinished)
	event.Err, event.Duration, event.Applied = err, duration, applied
	c.owner.emitEventLocked(event)
	if !applied {
		event.Kind, event.Reason = EventResultDiscarded, pending.discardReason
		if event.Reason == ReasonNone {
			event.Reason = ReasonCancel
		}
		c.owner.emitEventLocked(event)
	}
	c.finishRefreshLocked(key, pending, applied)
	c.notifyLocked(key)
	close(pending.done)
}

// run executes an ordinary loader or a page sequence through the same per-loader retry pipeline.
func (c *clientBucket) run(ctx context.Context, key any, pending *flight) (any, time.Time, error) {
	if pending.request.pages != nil {
		value, err := pending.request.pages(
			ctx,
			pending.previous,
			pending.request.nextPage,
			func(load Loader[any]) (any, error) {
				value, _, err := c.runLoader(ctx, key, pending, load)
				return value, err
			},
		)
		return value, time.Time{}, err
	}
	return c.runLoader(ctx, key, pending, pending.request.load)
}

// runLoader applies the same retry policy to a single query loader or an individual page.
func (c *clientBucket) runLoader(
	ctx context.Context,
	key any,
	pending *flight,
	load Loader[any],
) (any, time.Time, error) {
	options := pending.request.options
	var previousErr error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, time.Time{}, ctx.Err()
		}
		value, expiresAt, err := c.attempt(ctx, key, pending, load, previousErr)
		previousErr = err
		// Closed schedulers, overload, and recursive budgets cannot recover through retries.
		unavailable := errors.Is(err, ErrBatcherClosed) || errors.Is(err, ErrQueueFull)
		if unavailable || errors.Is(err, ErrExecutionReentry) {
			return value, expiresAt, err
		}
		stopped := ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		shouldRetry := err != nil && attempt < options.Retry && !stopped
		if !shouldRetry {
			return value, expiresAt, err
		}
		if options.RetryIf != nil && !options.RetryIf(err) {
			return value, expiresAt, err
		}
		if err := ctx.Err(); err != nil {
			return value, expiresAt, err
		}
		timer := time.NewTimer(options.RetryDelay(attempt + 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// attempt acquires execution capacity only around a loader invocation, never around retry backoff.
func (c *clientBucket) attempt(
	ctx context.Context,
	key any,
	pending *flight,
	load Loader[any],
	previousErr error,
) (any, time.Time, error) {
	// The initiating query owns budget selection; batch fetchers can skip this outer acquisition.
	ctx, release, err := pending.request.options.Executor.acquireContext(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer release()
	c.mu.Lock()
	pending.attempts++
	// A later page is a new initial attempt, not a retry of an earlier page.
	if previousErr != nil {
		c.stats.Retries++
		event := pending.event(key, EventRetryStarted)
		event.Err = previousErr
		c.owner.emitEventLocked(event)
	}
	c.mu.Unlock()
	if pending.request.shared != nil {
		forced := pending.reason == ReasonInvalidated || pending.reason == ReasonRefetch
		// Failed refreshes remain invalidated; reusing their old shared record would undo that invalidation.
		bypass := forced || pending.reason == ReasonPreviousFailure
		returnValue, sharedErr := pending.request.shared.load(ctx, bypass, func(tags []string) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.pending[key] != pending || c.owner.closed {
				return
			}
			cached := c.entryLocked(key)
			for _, tag := range tags {
				if !matchesQueryTags(cached.shared.tags, []string{tag}) {
					cached.shared.tags = append(cached.shared.tags, tag)
				}
			}
			c.storeEntryLocked(key, cached)
		})
		if sharedErr != nil {
			return nil, time.Time{}, sharedErr
		}
		return returnValue, time.Time{}, nil
	}
	if load != nil {
		return load(ctx)
	}
	value, err := pending.request.fetcher.fetch(ctx)
	return value, time.Time{}, err
}

// snapshotLocked removes data beyond MaxAge and derives state without scheduling a request.
func (c *clientBucket) snapshotLocked(key any) Snapshot[any] {
	c.expireDataLocked(key)
	cached := c.entryLocked(key)
	return c.entrySnapshotLocked(key, cached, cached.expiresAt,
		cached.hasData && c.owner.options.Clock().Before(cached.expiresAt))
}

// entrySnapshotLocked projects an already-checked entry without repeating age or freshness checks.
func (c *clientBucket) entrySnapshotLocked(key any, cached entry, expiresAt time.Time, fresh bool) Snapshot[any] {
	state := Snapshot[any]{
		Data: cached.value, HasData: cached.hasData, Err: cached.err,
		Stale: !fresh, Fetching: c.pending[key] != nil, UpdatedAt: cached.updatedAt, ExpiresAt: expiresAt,
	}
	switch {
	case c.owner.closed:
		state.Status, state.Err = Error, ErrClosed
	case cached.err != nil:
		state.Status = Error
	case cached.hasData:
		state.Status = Success
	case state.Fetching:
		state.Status = Pending
	default:
		state.Status = Idle
	}
	return state
}

// notifyLocked publishes the latest state while the client lock prevents subscription closure races.
func (c *clientBucket) notifyLocked(key any) {
	if len(c.observers[key]) == 0 {
		return
	}
	state := c.snapshotLocked(key)
	for observer := range c.observers[key] {
		observer.publish(c.withFreshnessLocked(key, state, observer.request.options))
	}
}

// cancelLocked detaches the active load so canceled or obsolete results cannot overwrite current data.
func (c *clientBucket) cancelLocked(key any, reason EventReason) {
	if pending := c.pending[key]; pending != nil {
		pending.detached = true
		pending.discardReason = reason
		pending.cancel()
		c.cancelRefreshLocked(pending, reason)
		delete(c.pending, key)
	}
}

// checkLocked rejects closed clients, unsafe keys, and incompatible static result types without changing state.
func (c *clientBucket) checkLocked(key any, typ reflect.Type) error {
	_, err := c.checkedEntryLocked(key, typ)
	return err
}

// checkedEntryLocked validates admission and returns state owned by the caller's existing lock.
func (c *clientBucket) checkedEntryLocked(key any, typ reflect.Type) (*entry, error) {
	if c.owner.closed {
		return nil, ErrClosed
	}
	// Native strings are comparable and reflexive; other types retain complete value validation.
	if _, nativeString := key.(string); !nativeString {
		if key == nil {
			return nil, fmt.Errorf("%w: %T", ErrInvalidKey, key)
		}
		keyType := reflect.TypeOf(key)
		// Only composite values can hide non-comparable dynamic interface fields.
		// Avoid Value.Comparable's traversal machinery for scalar keys on the hot path.
		composite := keyType.Kind() == reflect.Struct || keyType.Kind() == reflect.Array
		if !keyType.Comparable() || composite && !reflect.ValueOf(key).Comparable() {
			return nil, fmt.Errorf("%w: %T", ErrInvalidKey, key)
		}
		// NaN-containing keys are comparable but cannot retrieve their own map entries.
		if key != key { //nolint:gocritic // Reject non-reflexive keys before any map access.
			return nil, fmt.Errorf("%w: %T", ErrInvalidKey, key)
		}
	}
	cached := c.entries[key]
	typeMismatch := typ != nil && cached != nil && cached.typ != nil && cached.typ != typ
	if typeMismatch {
		// Key value formatting can reenter Client APIs while mu is held; type formatting cannot.
		return nil, fmt.Errorf(
			"%w: key type %T contains %v, requested %v",
			ErrTypeMismatch,
			key,
			cached.typ,
			typ,
		)
	}
	return cached, nil
}

// bindLocked installs a static type before a result or flight can become visible to other callers.
func (c *clientBucket) bindLocked(key any, typ reflect.Type) error {
	if err := c.checkLocked(key, typ); err != nil {
		return err
	}
	cached := c.entryLocked(key)
	cached.typ = typ
	c.storeEntryLocked(key, cached)
	return nil
}

// discardLocked clears retained state, preserves active type bindings, and records an explicit removal cause.
// ReasonNone suppresses events for internal cleanup of empty, unowned type metadata.
func (c *clientBucket) discardLocked(key any, reason EventReason) {
	cached := c.entryLocked(key)
	c.forgetCapacityLocked(key)
	if len(c.observers[key]) > 0 || c.pending[key] != nil {
		c.storeEntryLocked(key, entry{typ: cached.typ, tags: cached.tags, shared: cached.shared})
	} else {
		delete(c.entries, key)
	}
	// Live handles keep empty type bindings after removal. Repeated Remove or Clear
	// must not report another deletion when neither data nor an error was retained.
	if (cached.hasData || cached.err != nil) && reason != ReasonNone {
		c.owner.emitEventLocked(Event{Key: key, Kind: EventCacheRemoved, Reason: reason})
	}
}

// observedRequestLocked selects an enabled handle deterministically by subscription order.
func (c *clientBucket) observedRequestLocked(key any) queryRequest {
	var selected *subscription
	for observer := range c.observers[key] {
		if observer.enabled && (selected == nil || observer.order < selected.order) {
			selected = observer
		}
	}
	if selected != nil {
		return selected.request
	}
	return queryRequest{}
}

// entryFreshnessLocked derives a consumer's deadline and cache reuse decision from an already-read entry.
// Callers hold mu and remove over-age data first; explicit deadlines and invalidation remain authoritative.
func (c *clientBucket) entryFreshnessLocked(cached entry, staleTime time.Duration) (time.Time, bool) {
	expiresAt := cached.expiresAt
	if !cached.hasData {
		return expiresAt, false
	}
	if cached.ordinary && !cached.invalidated {
		expiresAt = c.ordinaryExpiry(cached, staleTime)
	}
	return expiresAt, c.owner.options.Clock().Before(expiresAt)
}

// ordinaryExpiry applies consumer freshness to source time and caps it by shared and local age limits.
func (c *clientBucket) ordinaryExpiry(cached entry, staleTime time.Duration) time.Time {
	start := cached.updatedAt
	if !cached.sharedExpiresAt.IsZero() {
		start = cached.fetchedAt
	}
	expiresAt := start.Add(staleTime)
	if !cached.sharedExpiresAt.IsZero() && expiresAt.After(cached.sharedExpiresAt) {
		expiresAt = cached.sharedExpiresAt
	}
	return c.limitExpiry(cached.updatedAt, expiresAt)
}

// withFreshnessLocked applies one consumer's freshness to ordinary, non-invalidated shared data.
func (c *clientBucket) withFreshnessLocked(key any, state Snapshot[any], options Options) Snapshot[any] {
	cached := c.entryLocked(key)
	applyPolicy := cached.ordinary && state.HasData && !cached.invalidated
	if applyPolicy {
		expiresAt, fresh := c.entryFreshnessLocked(cached, options.StaleTime)
		state.ExpiresAt, state.Stale = expiresAt, !fresh
	}
	return state
}
