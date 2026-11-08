package soroi

// touchCapacityLocked records use of a completed result or error and enforces the configured capacity.
func (c *clientBucket) touchCapacityLocked(key any) {
	if c.owner.closed || c.owner.options.MaxEntries <= 0 {
		return
	}
	cached := c.entryLocked(key)
	if !cached.hasData && cached.err == nil {
		c.forgetCapacityLocked(key)
		return
	}
	if position := c.owner.recencyEntries[key]; position != nil {
		c.owner.recency.MoveToBack(position)
	} else {
		c.owner.recencyEntries[key] = c.owner.recency.PushBack(key)
	}
	for c.owner.recency.Len() > c.owner.options.MaxEntries {
		oldest := c.owner.recency.Front().Value
		bucket := c.owner.bucketFor(oldest)
		bucket.stats.LRUEvictions++
		// Retain the type binding while a handle or load still owns this key.
		// Eviction clears only cached state and never cancels shared work or starts a refresh.
		bucket.discardLocked(oldest, ReasonCapacity)
		bucket.stopGCLocked(oldest)
		bucket.notifyLocked(oldest)
	}
}

// forgetCapacityLocked releases a key's recency record when its retained state is discarded.
func (c *clientBucket) forgetCapacityLocked(key any) {
	// Unbounded clients never register recency records, so cleanup can skip the index lookup.
	if c.owner.options.MaxEntries <= 0 {
		return
	}
	if position := c.owner.recencyEntries[key]; position != nil {
		c.owner.recency.Remove(position)
		delete(c.owner.recencyEntries, key)
	}
}
