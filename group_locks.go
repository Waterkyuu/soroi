package soroi

import "slices"

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
	sizes := make([]int, len(c.buckets))
	for i := range c.buckets {
		bucket := &c.buckets[i]
		if bucket.groupRefs == 0 {
			continue
		}
		sizes[groupRoot(parents, i)]++
	}
	var componentCount, memberCount int
	for _, size := range sizes {
		if size > 1 {
			componentCount++
			memberCount += size
		}
	}
	// One allocation owns all sets and another owns their disjoint pointer ranges.
	// This avoids per-component allocations when many independent groups are active.
	sets := make([]bucketLockSet, componentCount)
	members := make([]*clientBucket, memberCount)
	indexes := make([]int, len(c.buckets))
	var nextSet, offset int
	for root, size := range sizes {
		if size <= 1 {
			continue
		}
		sets[nextSet].buckets = members[offset : offset : offset+size]
		indexes[root] = nextSet + 1
		nextSet++
		offset += size
	}
	for i := range c.buckets {
		bucket := &c.buckets[i]
		if bucket.groupRefs == 0 {
			continue
		}
		index := indexes[groupRoot(parents, i)]
		if index != 0 {
			set := &sets[index-1]
			set.buckets = append(set.buckets, bucket)
			bucket.groupLocks = set
		}
	}
}

// groupBuckets returns sorted distinct admitted owners, omitting groups confined to one bucket.
func groupBuckets(group *queriesGroup) []*clientBucket {
	var first *clientBucket
	multiple := false
	for _, member := range group.members {
		if member.err != nil {
			continue
		}
		if first == nil {
			first = member.bucket
		} else if first != member.bucket {
			multiple = true
			break
		}
	}
	if !multiple {
		return nil
	}
	buckets := make([]*clientBucket, 0, len(group.members))
	for _, member := range group.members {
		if member.err == nil {
			buckets = append(buckets, member.bucket)
		}
	}
	slices.SortFunc(buckets, compareGroupBuckets)
	return slices.Compact(buckets)
}

// compareGroupBuckets preserves the global write-lock order in immutable component lists.
func compareGroupBuckets(a, b *clientBucket) int { return a.index - b.index }

// addGroupLocksLocked merges only the components connected by a newly admitted group.
// The caller holds every bucket lock; removal continues to use the original full rebuild.
func (c *Client) addGroupLocksLocked(group *queriesGroup) {
	buckets := groupBuckets(group)
	if len(buckets) == 0 {
		return
	}
	c.mergeGroupLocksLocked(buckets)
}

// mergeGroupLocksLocked reuses an existing component or publishes one new ordered union without mutating old sets.
func (c *Client) mergeGroupLocksLocked(buckets []*clientBucket) {
	first := buckets[0].groupLocks
	same := first != nil
	for _, bucket := range buckets[1:] {
		if bucket.groupLocks != first {
			same = false
			break
		}
	}
	if same {
		return
	}
	seen := make(map[*bucketLockSet]bool)
	merged := make([]*clientBucket, 0, len(buckets))
	for _, bucket := range buckets {
		set := bucket.groupLocks
		if set == nil {
			merged = append(merged, bucket)
		} else if !seen[set] {
			seen[set] = true
			merged = append(merged, set.buckets...)
		}
	}
	slices.SortFunc(merged, compareGroupBuckets)
	set := &bucketLockSet{buckets: merged}
	for _, bucket := range merged {
		bucket.groupLocks = set
	}
}
