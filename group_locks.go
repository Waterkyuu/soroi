package soroi

// bucketLockSet owns an immutable, ordered component of buckets joined by live groups.
type bucketLockSet struct {
	// buckets includes every owner reachable through recursive group publications in Client order.
	buckets []*clientBucket
}

// lock excludes all component transitions using the same order as Client-wide operations.
func (s *bucketLockSet) lock() {
	for _, bucket := range s.buckets {
		bucket.mu.raw.Lock()
	}
}

// unlock releases the component in reverse order without consulting mutable group topology.
func (s *bucketLockSet) unlock() {
	for i := len(s.buckets) - 1; i >= 0; i-- {
		s.buckets[i].mu.raw.Unlock()
	}
}

// groupRoot compresses parent paths while constructing a new topology under every bucket lock.
func groupRoot(parents []int, index int) int {
	for parents[index] != index {
		parents[index] = parents[parents[index]]
		index = parents[index]
	}
	return index
}

// rebuildGroupLocksLocked precomputes transitive publications when group membership changes.
// Callers hold every bucket lock, so writers can validate set identity after ordered acquisition.
func (c *Client) rebuildGroupLocksLocked() {
	for i := range c.buckets {
		c.buckets[i].groupLocks = nil
	}
	if len(c.groups) == 0 {
		return
	}
	parents := make([]int, len(c.buckets))
	for i := range parents {
		parents[i] = i
	}
	for group := range c.groups {
		first := -1
		for _, member := range group.members {
			if member.err != nil {
				continue
			}
			root := groupRoot(parents, member.bucket.index)
			if first < 0 {
				first = root
				continue
			}
			parents[root] = groupRoot(parents, first)
		}
	}
	sets := make([]*bucketLockSet, len(c.buckets))
	for i := range c.buckets {
		bucket := &c.buckets[i]
		if bucket.groupRefs == 0 {
			continue
		}
		root := groupRoot(parents, i)
		if sets[root] == nil {
			sets[root] = &bucketLockSet{}
		}
		sets[root].buckets = append(sets[root].buckets, bucket)
	}
	for i := range c.buckets {
		if c.buckets[i].groupRefs == 0 {
			continue
		}
		set := sets[groupRoot(parents, i)]
		if set != nil && len(set.buckets) > 1 {
			c.buckets[i].groupLocks = set
		}
	}
}
