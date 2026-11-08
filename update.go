package soroi

import (
	"errors"
	"reflect"
)

// ErrNoData indicates an update attempted without usable cached data.
var ErrNoData = errors.New("query has no cached data")

// ErrUpdateConflict indicates the cached state changed while an update callback was running.
var ErrUpdateConflict = errors.New("query changed during update")

// ErrNoUpdater indicates an Update call without a transformation function.
var ErrNoUpdater = errors.New("query has no updater")

// Update transforms existing cached data and installs it only if that state has not changed.
// The callback runs exactly once, outside all cache locks, and may call Client APIs.
// Data is shared and immutable: copy maps, slices, and pointed-to values before modifying them.
// Missing or over-age data returns ErrNoData without calling update. Concurrent writes,
// invalidation, removal, or expiration return ErrUpdateConflict without applying the result.
// Callback errors are returned unchanged. Callers may explicitly retry conflicts.
// Success has Set semantics: it renews freshness and age, cancels older loads, and notifies observers.
//
// Example: after Set(client, "balance", 100), Update(client, "balance", addTen) returns
// nil and stores 110 if the entry is unchanged. If another writer changes that key while
// addTen runs, Update returns ErrUpdateConflict and leaves the newer writer's result intact.
// It does not automatically rerun addTen, which may have application-owned side effects.
//
// The three phases deliberately keep different data:
//  1. Lock the owner, validate once, perform required expiry work, and copy the immutable
//     value reference and this entry's revision. Do not carry *entry outside the lock.
//  2. Unlock and invoke the user's callback exactly once.
//  3. Lock again, find the current entry afresh, and compare its stored revision. Only then
//     pass that current node to setEntryLocked, avoiding another lookup within this phase.
//
// A write to another key in the same bucket advances the number generator, not this entry's
// stored revision, so it alone cannot make the update conflict.
func Update[V any](client *Client, key any, update func(V) (V, error)) error {
	bucket := client.bucketFor(key)
	bucket.mu.Lock()
	typ := reflect.TypeFor[V]()
	cached, err := bucket.checkedEntryLocked(key, typ)
	if err != nil {
		bucket.mu.Unlock()
		return err
	}
	// Preserve error precedence: closed/key/type errors above win over a missing updater,
	// and a missing updater is reported before examining whether usable data exists.
	if update == nil {
		bucket.mu.Unlock()
		return ErrNoUpdater
	}
	if bucket.expireDataLocked(key) {
		bucket.notifyLocked(key)
		// Maintenance and publication may discard storage; reacquire the current node under this lock.
		cached = bucket.entries[key]
	}
	if cached == nil || !cached.hasData {
		bucket.mu.Unlock()
		return ErrNoData
	}
	// Carry immutable callback input and a revision value, never mutable storage, across the unlock.
	previousValue, previousRevision := cached.value, cached.revision
	bucket.mu.Unlock()
	value, err := update(typedValue[V](previousValue))
	if err != nil {
		return err
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.owner.closed {
		return ErrClosed
	}
	if bucket.expireDataLocked(key) {
		bucket.notifyLocked(key)
	}
	// The callback can Remove/Clear and recreate the key, even with the same value or a
	// different type. The bucket's revision generator survives that cleanup. A fresh lookup
	// plus revision comparison rejects the replacement; pointer or value equality is not enough.
	current := bucket.entries[key]
	if current == nil || !current.hasData || current.revision != previousRevision {
		return ErrUpdateConflict
	}
	bucket.setEntryLocked(key, value, typ, current)
	return nil
}

// advanceRevisionLocked is called whenever a state transition must invalidate an in-flight Update.
// cached is the entry being changed and the caller holds its owner exclusively. It writes
// the next bucket-issued number into that entry alone; same-value writes still advance it.
// The generator is not reset by removal, so a recreated key cannot reuse the old revision.
func (c *clientBucket) advanceRevisionLocked(cached *entry) {
	cached.revision = c.nextDataRevision()
}
