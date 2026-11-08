package soroi

// entryLocked copies retained state so callers cannot carry mutable storage across unlocks.
func (c *clientBucket) entryLocked(key any) entry {
	if cached := c.entries[key]; cached != nil {
		return *cached
	}
	return entry{}
}

// storeEntryLocked reuses indexed storage and allocates only when a key first acquires state.
func (c *clientBucket) storeEntryLocked(key any, value entry) {
	if cached := c.entries[key]; cached != nil {
		*cached = value
		return
	}
	// Allocating inside the miss branch keeps the value parameter off the heap on overwrites.
	cached := new(entry)
	*cached = value
	// Empty buckets remain allocation-free until their first binding or result.
	if c.entries == nil {
		c.entries = make(map[any]*entry)
	}
	c.entries[key] = cached
}
