package soroi

import (
	"hash/maphash"
	"sync/atomic"
)

// defaultBucketCount bounds idle storage while separating ordinary concurrent key writes.
const defaultBucketCount = 64

// clientBucket owns every mutable part of a key lifecycle without creating another Client.
type clientBucket struct {
	// owner shares configuration, diagnostics and cancellation with all other buckets.
	owner *Client
	// mu protects this bucket or coordinates global policies when required.
	mu bucketMutex
	// readHits permits fresh Fetch decisions under the shared read lock.
	readHits atomic.Uint64
	// stats accumulates activity under the bucket write lock.
	stats Stats
	// entries retains stable result nodes; its index is allocated on the first binding.
	entries map[any]*entry
	// pending owns at most one active load per key and is allocated on the first load.
	pending map[any]*flight
	// observers owns live consumers and is allocated on the first subscription.
	observers map[any]map[*subscription]struct{}
	// gcTasks owns optional inactive-retention timers.
	gcTasks map[any]*gcTask
	// pollTasks owns optional polling timers.
	pollTasks map[any]*pollTask
	// groupRefs requires all-bucket coordination while a group reads this bucket in publications.
	groupRefs uint64
}

// bucketFor locates immutable key ownership before taking a lock; callers retain the returned pointer.
func (c *Client) bucketFor(key any) *clientBucket {
	// Construction guarantees a positive power-of-two length, so the mask is non-negative.
	mask := uint64(len(c.buckets) - 1) //nolint:gosec
	if text, ok := key.(string); ok {
		return &c.buckets[maphash.String(c.seed, text)&mask]
	}
	return &c.buckets[hashBucketKey(c.seed, key)&mask]
}

// forEachBucketLocked visits owners while the caller holds the Client's complete lock set.
func (c *Client) forEachBucketLocked(visit func(*clientBucket)) {
	for i := range c.buckets {
		visit(&c.buckets[i])
	}
}

// publishGroupLocked preserves complete group snapshots under the coordinated write lock.
func (c *clientBucket) publishGroupLocked(group *queriesGroup) { group.publishLocked() }

// nextLoadIdentity reserves a unique Client-wide identity without serializing unrelated keys.
func (c *Client) nextLoadIdentity() uint64 { return c.nextLoadID.Add(1) }

// nextDataRevision prevents removal and recreation from hiding an Update conflict.
func (c *Client) nextDataRevision() uint64 { return c.nextRevision.Add(1) }

// nextObserverOrder preserves admission ordering for automatic same-key loader selection.
func (c *Client) nextObserverOrder() uint64 { return c.nextObserver.Add(1) - 1 }

// hasEvents permits subscription-free readers to avoid the mutable diagnostic index.
func (c *Client) hasEvents() bool { return c.eventCount.Load() != 0 }
