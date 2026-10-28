package soroi

import (
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

// QueriesHandle owns independent subscriptions and one ordered or caller-combined update stream.
// Close releases only this group's subscriptions, without canceling shared work.
type QueriesHandle[V any] struct {
	// group owns the shared raw subscription state and its lifetime.
	group *queriesGroup
	// updates carries the latest output; only a combined handle owns a projection worker.
	updates <-chan V
	// combine transforms independent states outside the Client lock; default handles return the slice unchanged.
	combine func([]Snapshot[any]) V
}

// queriesGroup owns raw subscriptions independently of the chosen public output type.
type queriesGroup struct {
	// client serializes admission, state reads, publications, and subscription release.
	client *Client
	// members preserves input order, captured policies, and construction failures.
	members []queriesMember
	// updates carries copied ordered states without blocking loaders.
	updates chan []Snapshot[any]
	// initialized prevents construction-time notifications from publishing incomplete groups.
	initialized bool
	// remaining counts admitted subscriptions until group or client closure releases them.
	remaining int
	// closed prevents publication after the final subscription releases the raw channel.
	closed bool
	// publishing prevents age-expiration notifications from recursively rebuilding the same group.
	publishing bool
}

// queriesMember owns one definition's subscription or its construction failure.
type queriesMember struct {
	// spec retains the captured identity, result type, and loading policy.
	spec querySpec
	// observer is registered only if validation succeeds.
	observer *subscription
	// err retains construction failures independently of later cache changes.
	err error
}

// Queries subscribes to independent typed definitions and returns their states in input order.
// Each definition follows Query's caching, retry, polling, and validation rules.
// Duplicate keys share data and in-flight work while retaining separate result positions.
// Nil definitions report ErrNoFetcher. Construction failures appear in their result positions.
// Empty or entirely invalid groups publish once and close Updates immediately.
// The caller owns Close; use QueriesWithCombine for a caller-defined output type.
func Queries(client *Client, definitions ...QuerySpec) *QueriesHandle[[]Snapshot[any]] {
	group := newQueriesGroup(client, definitions)
	return &QueriesHandle[[]Snapshot[any]]{
		group:   group,
		updates: group.updates,
		combine: func(results []Snapshot[any]) []Snapshot[any] { return results },
	}
}

// QueriesWithCombine subscribes like Queries and projects each state slice to the caller's result type.
// combine runs outside the Client lock and must return promptly. Updates are projected serially by one worker;
// Snapshot calls may invoke combine concurrently, so callbacks must synchronize captured mutable state.
// Slow callbacks and readers may skip intermediate states. After Close, already-captured updates finish
// projection before Updates closes; Close itself does not wait for the callback.
// A nil combine panics before subscriptions are created.
func QueriesWithCombine[V any](
	client *Client,
	combine func([]Snapshot[any]) V,
	definitions ...QuerySpec,
) *QueriesHandle[V] {
	if combine == nil {
		panic("soroi: nil queries combine function")
	}
	group := newQueriesGroup(client, definitions)
	updates := make(chan V, 1)
	// One worker owns projection and output closure so user code never executes under Client.mu.
	go func() {
		defer close(updates)
		for results := range group.updates {
			publishLatest(updates, combine(results))
		}
	}()
	return &QueriesHandle[V]{group: group, updates: updates, combine: combine}
}

// newQueriesGroup captures external configuration before atomically admitting the ordered subscriptions.
func newQueriesGroup(client *Client, definitions []QuerySpec) *queriesGroup {
	group := &queriesGroup{
		client:  client,
		members: make([]queriesMember, len(definitions)),
		updates: make(chan []Snapshot[any], 1),
	}
	for i, definition := range definitions {
		member := &group.members[i]
		value := reflect.ValueOf(definition)
		nilDefinition := !value.IsValid() || (value.Kind() == reflect.Pointer && value.IsNil())
		if nilDefinition {
			member.err = ErrNoFetcher
			continue
		}
		// Shared configuration can call an application-owned store's InvalidationScope method.
		// Resolve it before locking, allowing diagnostics to reenter Client APIs safely.
		member.spec = definition.querySpec(client.options)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for i := range group.members {
		member := &group.members[i]
		if member.err != nil {
			continue
		}
		member.observer = &subscription{
			publish: func(Snapshot[any]) {
				group.publishLocked()
			},
			close: func() {
				group.remaining--
				if group.remaining == 0 {
					group.closed = true
					close(group.updates)
				}
			},
		}
		member.err = client.subscribeLocked(member.spec, member.observer)
		if member.err == nil {
			group.remaining++
		}
	}
	group.initialized = true
	group.publishLocked()
	if group.remaining == 0 {
		group.closed = true
		close(group.updates)
	}
	return group
}

// Snapshot reads current states without loading, then applies the optional projection outside the Client lock.
// Released groups can still read shared cache data; closed clients report ErrClosed for admitted members.
func (h *QueriesHandle[V]) Snapshot() V {
	return h.combine(h.group.snapshot())
}

// Updates returns the initial and subsequent latest output values; intermediate states may be skipped.
// The channel closes after subscription release and any outstanding combine callbacks finish.
func (h *QueriesHandle[V]) Updates() <-chan V { return h.updates }

// Close releases this group's subscriptions once without canceling loads or other same-key consumers.
// Combined output closure waits for callbacks asynchronously; this method does not wait for them.
func (h *QueriesHandle[V]) Close() {
	group := h.group
	group.client.mu.Lock()
	defer group.client.mu.Unlock()
	if group.closed {
		return
	}
	for _, member := range group.members {
		if member.err == nil {
			group.client.unsubscribeLocked(member.spec.key, member.observer)
		}
	}
}

// snapshot serializes a complete group read before the caller invokes application code.
func (g *queriesGroup) snapshot() []Snapshot[any] {
	g.client.mu.Lock()
	defer g.client.mu.Unlock()
	return g.snapshotLocked()
}

// snapshotLocked builds each result from current cache state using its captured policy.
func (g *queriesGroup) snapshotLocked() []Snapshot[any] {
	results := make([]Snapshot[any], len(g.members))
	for i, member := range g.members {
		if member.err != nil {
			results[i] = Snapshot[any]{Status: Error, Stale: true, Err: member.err}
			continue
		}
		results[i] = g.client.getSnapshotLocked(member.spec.key, member.spec.typ, member.spec.request.options)
	}
	return results
}

// publishLocked rebuilds ordered results using the same age and freshness rules as explicit reads.
func (g *queriesGroup) publishLocked() {
	ready := g.initialized && !g.closed
	if !ready || g.publishing {
		return
	}
	// Reading one member can expire data and notify its observers, including this group.
	// Suppress recursive publications while the outer read collects every member's current state.
	g.publishing = true
	defer func() { g.publishing = false }()
	publishLatest(g.updates, g.snapshotLocked())
}
