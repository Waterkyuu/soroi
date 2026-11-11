package soroi

import "sync"

// clientMutex is the all-bucket coordinator, not an additional mutex ahead of every key.
// Clear, Close, aggregate operations, and group admission/closure acquire bucket 0, then 1,
// and so on. Ordinary key operations go directly to bucketMutex below. Every operation
// needing multiple buckets uses the same increasing order to avoid circular waits.
type clientMutex struct {
	// owner owns the stable lock array for this mutex's entire lifetime.
	owner *Client
}

// Lock excludes every key transition without a shared gate on ordinary reads and writes.
func (m *clientMutex) Lock() {
	for i := range m.owner.buckets {
		m.owner.buckets[i].mu.raw.Lock()
	}
}

// Unlock releases a complete lock set in reverse order.
func (m *clientMutex) Unlock() {
	for i := len(m.owner.buckets) - 1; i >= 0; i-- {
		m.owner.buckets[i].mu.raw.Unlock()
	}
}

// TryLock acquires every bucket immediately or releases the partial acquisition.
func (m *clientMutex) TryLock() bool {
	for i := range m.owner.buckets {
		if m.owner.buckets[i].mu.raw.TryLock() {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			m.owner.buckets[j].mu.raw.Unlock()
		}
		return false
	}
	return true
}

// bucketMutex chooses how many owners an exclusive operation must protect.
// Normally only this key's bucket is needed. A grouped notification can read other owners,
// so its write must protect the whole connected group. Global LRU capacity and a supplied
// clock retain all-bucket serialization. This wrapper never replaces the underlying lock.
// Fields all and held describe the current writer's acquired locks, not cached topology.
type bucketMutex struct {
	// raw protects both key state and the selected write-lock mode.
	raw sync.RWMutex
	// bucket supplies immutable policy ownership and lock-protected group membership.
	bucket *clientBucket
	// all records this writer's acquired lock set and is protected by raw exclusively.
	all bool
	// held records the immutable group lock set acquired by this writer under raw exclusively.
	held *bucketLockSet
}

// Lock is called before a key operation that can change cache state or publish updates.
// It first acquires the local raw lock to inspect protected topology. For an ungrouped
// key it returns holding only that lock; capacity/custom-clock policies acquire all owners.
// For a grouped key it drops the local lock, acquires the ordered immutable component,
// and checks that the component is still current. Admission or closure may have changed
// it during the gap; in that case release the old set and retry before touching any data.
// Keeping the local lock while acquiring an earlier bucket would invert the global order.
func (m *bucketMutex) Lock() {
	m.raw.Lock()
	owner := m.bucket.owner
	globalPolicy := owner.options.MaxEntries > 0 || !owner.concurrentClock
	if globalPolicy {
		m.raw.Unlock()
		owner.mu.Lock()
		m.all = true
		return
	}
	for {
		set := m.bucket.groupLocks
		if set == nil {
			return
		}
		// Release before ordered acquisition; topology changes may replace this set in the gap.
		m.raw.Unlock()
		set.lock()
		if m.bucket.groupLocks == set {
			m.held = set
			return
		}
		// Retry from the current topology while still preserving one global lock order.
		set.unlock()
		m.raw.Lock()
	}
}

// Unlock finishes the matching exclusive operation. It releases the exact all-bucket,
// component, or single-bucket locks selected by Lock, in reverse acquisition order.
// Never reconstruct this list from current topology: a writer must release what it owns.
func (m *bucketMutex) Unlock() {
	if m.all {
		m.all = false
		m.bucket.owner.mu.Unlock()
		return
	}
	if m.held != nil {
		set := m.held
		m.held = nil
		set.unlock()
		return
	}
	m.raw.Unlock()
}

// RLock is used only by read paths whose policies require no mutation or group publication.
// It takes the owner's ordinary RWMutex read lock; reader registration still exists within
// that bucket. The optimization changes lock ownership; it does not make reads lock-free.
func (m *bucketMutex) RLock() { m.raw.RLock() }

// RUnlock completes a side-effect-free local read.
func (m *bucketMutex) RUnlock() { m.raw.RUnlock() }
