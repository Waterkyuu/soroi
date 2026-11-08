package soroi

// entryLocked reads one key while its bucket is locked and returns a value copy of its record.
// For a missing key it returns entry{}; for a retained key it copies the metadata and the
// immutable value reference. It does not deep-copy application maps, slices, or pointers.
// Callers needing a snapshot can carry this copy across an unlock without retaining a
// mutable *entry; application-owned data must still follow the shared-immutable-data contract.
func (c *clientBucket) entryLocked(key any) entry {
	if cached := c.entries[key]; cached != nil {
		return *cached
	}
	return entry{}
}

// storeEntryLocked is called by lifecycle transitions holding this bucket exclusively.
// It installs value at key, preserving the existing node address when key is already indexed.
// For example, replacing "balance" from 100 to 110 overwrites its record rather than
// allocating another node. The map and node are allocated only for a missing binding.
// Readers can safely dereference the node only while holding this bucket's lock.
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
