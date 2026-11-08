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
// The callback runs exactly once, outside the Client lock, and may call Client APIs.
// Data is shared and immutable: copy maps, slices, and pointed-to values before modifying them.
// Missing or over-age data returns ErrNoData without calling update. Concurrent writes,
// invalidation, removal, or expiration return ErrUpdateConflict without applying the result.
// Callback errors are returned unchanged. Callers may explicitly retry conflicts.
// Success has Set semantics: it renews freshness and age, cancels older loads, and notifies observers.
func Update[V any](client *Client, key any, update func(V) (V, error)) error {
	bucket := client.bucketFor(key)
	bucket.mu.Lock()
	if err := bucket.checkLocked(key, reflect.TypeFor[V]()); err != nil {
		bucket.mu.Unlock()
		return err
	}
	if update == nil {
		bucket.mu.Unlock()
		return ErrNoUpdater
	}
	if bucket.expireDataLocked(key) {
		bucket.notifyLocked(key)
	}
	previous := bucket.entryLocked(key)
	bucket.mu.Unlock()
	if !previous.hasData {
		return ErrNoData
	}
	value, err := update(typedValue[V](previous.value))
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
	current := bucket.entryLocked(key)
	if !current.hasData || current.revision != previous.revision {
		return ErrUpdateConflict
	}
	bucket.setLocked(key, value, reflect.TypeFor[V]())
	return nil
}

// advanceRevisionLocked assigns a bucket-lifetime revision so recreation cannot hide same-key conflicts.
func (c *clientBucket) advanceRevisionLocked(cached *entry) {
	cached.revision = c.nextDataRevision()
}
