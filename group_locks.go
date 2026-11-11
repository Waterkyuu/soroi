package soroi

import "slices"

// bucketLockSet is the complete ordered list of owners a grouped write must protect.
// If groups connect A-B and B-C, a write to A can publish B and then reach C through
// expiration notifications, so the list must contain A, B, and C. A direct-neighbor list
// is insufficient. Bucket numbers, not group input order, define lock acquisition order.
//
// Once published through bucket.groupLocks, the set and its backing slice are immutable.
// A writer may retain an old pointer while dropping its first lock and acquiring the set.
// Topology changes allocate replacement sets; client_lock.go verifies pointer identity
// after acquisition and retries if necessary. Mutating an old slice would break that protocol.
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

// groupRoot is the full-rebuild helper that returns an owner's connected-component root.
// parents maps bucket index to parent bucket index and is local to one rebuild. For example,
// parents=[0, 0, 1] and index=2 returns 0 while shortening the traversed parent chain.
// Only this temporary array changes; published lock sets are never used as scratch space.
func groupRoot(parents []int, index int) int {
	for parents[index] != index {
		parents[index] = parents[parents[index]]
		index = parents[index]
	}
	return index
}

// rebuildGroupLocksLocked is called when the last admitted member of a group closes.
// Removing a connecting group may split A-B-C-D back into A-B and C-D. Under every bucket
// lock, rebuild from the remaining admitted groups rather than assuming the old connection
// survives. Invalid definitions do not participate; duplicate valid members still contribute
// their existing membership counts. Singleton owners need no multi-bucket lock set.
//
// The algorithm deliberately uses temporary index arrays and compact allocations for all
// resulting sets. New-group admission uses addGroupLocksLocked below, but removal retains
// this complete calculation. It clears/replaces bucket pointers without mutating old sets,
// allowing waiting writers to reject outdated pointers after they acquire their locks.
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
	// Each set gets a disjoint range in one backing slice. No published slice is extended
	// afterward; writes holding an older set may still need its original ordered range.
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

// groupBuckets is called after newQueriesGroup has admitted all valid definitions.
// It returns this group's distinct owner pointers sorted by bucket index. For example,
// admitted owner indexes [7, 2, 7] become [2, 7]; [2, 2] returns nil because a group confined
// to one bucket creates no cross-bucket relationship. Failed definitions are ignored.
// Deduplication applies only to lock ownership: group.members retains every input position
// and therefore still produces separate results for repeated keys.
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

// compareGroupBuckets is the sorting callback used for new owner lists. It returns a
// negative number for (bucket 2, bucket 7), zero for the same index, and a positive number
// for the reverse order. This matches Client-wide acquisition order in client_lock.go.
func compareGroupBuckets(a, b *clientBucket) int { return a.index - b.index }

// addGroupLocksLocked is called once after a new live group is added to Client.groups.
// group contains the admission results; all bucket locks remain held until initial
// publication is complete. Only the components touched by its distinct valid owners are
// merged. No reverse index or persistent group metadata is required for this operation.
// Adding a relationship cannot split an existing component; removal is handled separately
// by rebuildGroupLocksLocked, which must not be replaced with this merge-only operation.
func (c *Client) addGroupLocksLocked(group *queriesGroup) {
	buckets := groupBuckets(group)
	if len(buckets) == 0 {
		return
	}
	c.mergeGroupLocksLocked(buckets)
}

// mergeGroupLocksLocked receives at least two distinct, sorted owners from groupBuckets.
// The caller holds every bucket exclusively. If every owner already belongs to one set,
// no relationship changes and the existing pointer is retained. Otherwise it publishes
// the ordered union of all touched components while leaving unrelated sets untouched.
// For example, existing [0, 2] and [5, 7] plus a new group [2, 5] produce [0, 2, 5, 7].
// The set map below avoids copying a component twice when several members already belong
// to it. Existing components are disjoint, so distinct-component expansion cannot duplicate
// an owner. The new slice owns its storage; append must never reuse an old set's backing array.
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
	// seen maps each old component pointer to whether its complete owner list was copied.
	// It is temporary admission state, not a global registry or a reader counter.
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
