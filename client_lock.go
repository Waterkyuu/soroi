package soroi

import "sync"

// clientMutex acquires bucket locks in a fixed order for coherent global operations.
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

// bucketMutex selects local or coordinated writes without changing key ownership.
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

// Lock takes one bucket, its transitive group owners, or every bucket for global policies.
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

// Unlock releases the acquired set even if group membership changed before acquisition.
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

// RLock shares side-effect-free reads without a Client-wide reader count.
func (m *bucketMutex) RLock() { m.raw.RLock() }

// RUnlock completes a side-effect-free local read.
func (m *bucketMutex) RUnlock() { m.raw.RUnlock() }
