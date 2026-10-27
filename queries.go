package soroi

import (
	"errors"
	"reflect"
	"time"
)

// QuerySpec is a typed Definition accepted by Queries, including definitions with different result types.
// Pass Definition values directly; this interface is implemented by soroi's definitions only.
type QuerySpec interface {
	// querySpec captures the concrete definition's static type and copied policy.
	querySpec(Options) querySpec
}

// querySpec retains the static type and resolved policy needed to admit one subscription.
type querySpec struct {
	// key identifies the shared cache and in-flight request.
	key any
	// typ preserves the definition's static result type when results are exposed as any.
	typ reflect.Type
	// request owns the copied loading policy, tags, and optional shared-cache binding.
	request queryRequest
	// disable prevents this subscription from initiating automatic loads.
	disable bool
	// refetchInterval is the copied cadence for this subscription's polling.
	refetchInterval time.Duration
}

// QueriesSnapshot contains independent results in the order supplied to Queries.
// Each result's Data keeps its dynamic type and must be treated as immutable.
type QueriesSnapshot struct {
	// Results contains one snapshot per input definition; the slice belongs to the receiver.
	Results []Snapshot[any]
	// HasData reports whether every result has usable data; true for an empty group.
	HasData bool
	// Fetching reports whether any result has an active load, including background refreshes.
	Fetching bool
	// Err joins all result errors in input order and supports errors.Is and errors.As.
	// One failed query neither cancels its siblings nor discards their results.
	Err error
}

// QueriesHandle owns a fixed group of independent subscriptions and one combined update stream.
// Close releases only this group's subscriptions, without canceling shared work.
type QueriesHandle struct {
	// client serializes member state, publications, and subscription lifetimes under its mutex.
	client *Client
	// members preserves input order, construction errors, and each subscriber's latest notification.
	members []queriesMember
	// updates retains the latest aggregate without blocking loaders.
	updates chan QueriesSnapshot
	// initialized prevents construction-time notifications from publishing incomplete groups.
	initialized bool
	// remaining counts admitted subscriptions until group or client closure releases them.
	remaining int
	// closed prevents publication after the final subscription releases the output channel.
	closed bool
}

// queriesMember owns one definition's subscription or its construction failure.
type queriesMember struct {
	// spec retains the captured identity, result type, and loading policy.
	spec querySpec
	// observer is registered only if validation succeeds.
	observer *subscription
	// err retains construction failures independently of later cache changes.
	err error
	// state stores the last notification without sharing the caller's aggregate result slice.
	state Snapshot[any]
}

// Queries subscribes to independent typed definitions without waiting for their loads.
// Each definition follows Query's caching, retry, polling, and validation rules.
// Results preserve input order, including duplicate keys and per-query construction failures.
// Nil definitions report ErrNoFetcher in their result positions.
// An empty or entirely invalid group publishes once and closes Updates immediately.
// The group owns its subscriptions; close it when the consumer leaves.
func Queries(client *Client, definitions ...QuerySpec) *QueriesHandle {
	handle := &QueriesHandle{
		client:  client,
		members: make([]queriesMember, len(definitions)),
		updates: make(chan QueriesSnapshot, 1),
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for i, definition := range definitions {
		member := &handle.members[i]
		value := reflect.ValueOf(definition)
		nilDefinition := !value.IsValid() || (value.Kind() == reflect.Pointer && value.IsNil())
		if nilDefinition {
			member.err = ErrNoFetcher
		} else {
			member.spec = definition.querySpec(client.options)
			member.observer = &subscription{
				publish: func(state Snapshot[any]) {
					member.state = state
					handle.publishLocked()
				},
				close: func() {
					handle.remaining--
					if handle.remaining == 0 {
						handle.closed = true
						close(handle.updates)
					}
				},
			}
			member.err = client.subscribeLocked(member.spec, member.observer)
		}
		if member.err != nil {
			member.state = Snapshot[any]{Status: Error, Stale: true, Err: member.err}
			continue
		}
		handle.remaining++
	}
	handle.initialized = true
	handle.publishLocked()
	if handle.remaining == 0 {
		handle.closed = true
		close(handle.updates)
	}
	return handle
}

// Snapshot reads every member's current state using its own freshness policy without starting loads.
// Released groups can still read shared cache data; closed clients report ErrClosed for admitted members.
func (h *QueriesHandle) Snapshot() QueriesSnapshot {
	h.client.mu.Lock()
	defer h.client.mu.Unlock()
	results := make([]Snapshot[any], len(h.members))
	for i, member := range h.members {
		results[i] = member.state
		if member.err == nil {
			results[i] = h.client.getSnapshotLocked(member.spec.key, member.spec.typ, member.spec.request.options)
		}
	}
	return combineQueries(results)
}

// Updates returns initial and subsequent combined states; slow readers may skip intermediate transitions.
// The channel closes when the group or its Client closes, after any buffered state is read.
func (h *QueriesHandle) Updates() <-chan QueriesSnapshot { return h.updates }

// Close releases all of this group's subscriptions once, including their polling and retention ownership.
// In-flight requests and other consumers of the same keys remain owned by the Client.
func (h *QueriesHandle) Close() {
	h.client.mu.Lock()
	defer h.client.mu.Unlock()
	if h.closed {
		return
	}
	for _, member := range h.members {
		if member.err == nil {
			h.client.unsubscribeLocked(member.spec.key, member.observer)
		}
	}
}

// publishLocked copies current member states before replacing the bounded aggregate channel.
func (h *QueriesHandle) publishLocked() {
	if !h.initialized || h.closed {
		return
	}
	results := make([]Snapshot[any], len(h.members))
	for i, member := range h.members {
		results[i] = member.state
	}
	publishLatest(h.updates, combineQueries(results))
}

// combineQueries derives readiness, activity, and joined errors without hiding individual outcomes.
func combineQueries(results []Snapshot[any]) QueriesSnapshot {
	state := QueriesSnapshot{Results: results, HasData: true}
	errs := []error{}
	for _, result := range results {
		state.HasData = state.HasData && result.HasData
		state.Fetching = state.Fetching || result.Fetching
		if result.Err != nil {
			errs = append(errs, result.Err)
		}
	}
	state.Err = errors.Join(errs...)
	return state
}
