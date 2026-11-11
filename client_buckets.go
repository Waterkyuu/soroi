package soroi

import (
	"hash/maphash"
	"sync/atomic"
)

// defaultBucketCount creates 64 storage owners inside one Client, not 64 independent clients.
// Each owner has its own entries, loaders, observers, and lock. Unrelated keys can therefore
// write concurrently when they land in different buckets. A hot key still has one owner.
// Keep the count fixed for the Client lifetime: moving a key would invalidate retained
// handle pointers, pending loads, and the bucket-local revision checks used by Update.
// The count is internal; capacity and custom-clock guarantees still use coordinated locks.
const defaultBucketCount = 64

// clientBucket owns every mutable part of a key lifecycle without creating another Client.
// Public APIs route a key once through bucketFor; handles and asynchronous loads retain
// that pointer. The shared owner still supplies configuration, cancellation, diagnostics,
// and global capacity bookkeeping. A bucket is never copied after its mutex is initialized.
//
// Call flow for Set(client, "balance", 100):
//  1. bucketFor selects the same bucket for every equal key during this Client's lifetime.
//  2. bucket.mu.Lock selects a local, group-wide, or Client-wide exclusive lock set.
//  3. checkedEntryLocked validates the key/type and returns its current entry once.
//  4. setEntryLocked updates that node, advances its revision, and publishes notifications.
//  5. bucket.mu.Unlock releases exactly the lock set acquired in step 2.
type clientBucket struct {
	// index orders this bucket in immutable Client storage and precomputed lock sets.
	index int
	// owner shares configuration, diagnostics and cancellation with all other buckets.
	owner *Client
	// mu protects this bucket or coordinates global policies when required.
	mu bucketMutex
	// readHits permits fresh Fetch decisions under the shared read lock.
	readHits atomic.Uint64
	// stats accumulates activity under the bucket write lock.
	stats Stats
	// entries maps each key to one mutable record protected by mu; a record is not a read-only copy.
	// Updating a retained key reuses its node. Removal may destroy it, so callers must not
	// retain a node pointer across an unlock. Nil maps avoid allocating for unused buckets.
	entries map[any]*entry
	// pending owns at most one active load per key and is allocated on the first load.
	pending map[any]*flight
	// observers owns live consumers and is allocated on the first subscription.
	observers map[any]map[*subscription]struct{}
	// gcTasks owns optional inactive-retention timers.
	gcTasks map[any]*gcTask
	// pollTasks owns optional polling timers.
	pollTasks map[any]*pollTask
	// groupRefs counts admitted member positions, not distinct groups or distinct keys.
	// Queries(A, A) contributes two memberships. Group creation/closure updates this
	// under all bucket locks; it is neither the RWMutex reader count nor an Update revision.
	groupRefs uint64
	// groupLocks covers transitive group publications and is replaced only under every bucket lock.
	groupLocks *bucketLockSet
	// nextRevision supplies write identities while this bucket is exclusively locked.
	// The assigned number is stored in each changed entry; it is not one shared version
	// that makes every key conflict. Remove and Clear must leave this counter intact.
	nextRevision uint64
}

// bucketFor is called by public operations before locking and returns the stable owner of key.
// For example, repeated bucketFor("balance") calls return the same *clientBucket within
// this Client; another Client can choose a different bucket because its hash seed differs.
// This only routes the key: checkedEntryLocked must still reject invalid keys before map access.
// String keys use a direct hash; other supported keys preserve Go equality in client_hash.go.
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

// nextDataRevision is called by advanceRevisionLocked during an exclusive state change.
// It returns the next bucket-local number: a counter of 7 becomes 8 and returns 8.
// No atomic operation is needed because bucket.mu already excludes another writer.
//
// Example: A keeps revision 7 while a write to B takes number 8. Update(A) still compares
// A's stored revision with 7, so B cannot cause a false conflict merely by sharing the bucket.
// A later write to A takes a new number even when the value is unchanged. Keeping the counter
// across Remove/Clear also distinguishes a recreated A from the entry an updater first read.
func (c *clientBucket) nextDataRevision() uint64 {
	c.nextRevision++
	return c.nextRevision
}

// nextObserverOrder preserves admission ordering for automatic same-key loader selection.
func (c *Client) nextObserverOrder() uint64 { return c.nextObserver.Add(1) - 1 }

// hasEvents permits subscription-free readers to avoid the mutable diagnostic index.
func (c *Client) hasEvents() bool { return c.eventCount.Load() != 0 }
