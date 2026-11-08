package soroi

// clientBucket names lock-owned key storage while it is extracted from Client.
type clientBucket = Client

// bucketFor resolves key ownership; the extraction step still has one storage owner.
func (c *Client) bucketFor(_ any) *clientBucket { return c }

// forEachBucketLocked visits storage owners while the caller excludes all state transitions.
func (c *Client) forEachBucketLocked(visit func(*clientBucket)) { visit(c) }

// deferGroupLocked preserves group publication while call sites adopt explicit storage ownership.
func (c *clientBucket) deferGroupLocked(group *queriesGroup) { group.publishLocked() }

// nextLoadIdentity reserves a Client-wide identity for a new shared load.
func (c *Client) nextLoadIdentity() uint64 {
	c.nextLoadID++
	return c.nextLoadID
}

// nextDataRevision prevents removal and recreation from hiding an Update conflict.
func (c *Client) nextDataRevision() uint64 {
	c.nextRevision++
	return c.nextRevision
}

// nextObserverOrder preserves subscription admission order for automatic loader selection.
func (c *Client) nextObserverOrder() uint64 {
	order := c.nextObserver
	c.nextObserver++
	return order
}

// hasEvents reports whether diagnostic decisions need event publication.
func (c *Client) hasEvents() bool { return c.eventCount.Load() != 0 }
