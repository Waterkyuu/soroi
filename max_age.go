package soroi

import "time"

// limitExpiry prevents a freshness deadline from extending beyond the configured availability limit.
func (c *clientBucket) limitExpiry(updatedAt, expiresAt time.Time) time.Time {
	if c.owner.options.MaxAge > 0 {
		deadline := updatedAt.Add(c.owner.options.MaxAge)
		if expiresAt.After(deadline) {
			return deadline
		}
	}
	return expiresAt
}

// overMaxAgeLocked compares a value's local installation time with the hard age limit.
// Its caller holds the bucket lock and preserves serialization for custom clocks.
func (c *clientBucket) overMaxAgeLocked(updatedAt time.Time) bool {
	maxAge := c.owner.options.MaxAge
	return maxAge > 0 && !c.owner.options.Clock().Before(updatedAt.Add(maxAge))
}

// expireDataLocked drops an over-age value while preserving its result type, last error, and active load.
// Availability is checked on cache operations rather than by starting another timer for each key.
func (c *clientBucket) expireDataLocked(key any) bool {
	if c.owner.options.MaxAge <= 0 {
		return false
	}
	cached := c.entryLocked(key)
	if !cached.hasData || !c.overMaxAgeLocked(cached.updatedAt) {
		return false
	}
	// A failed refresh changes the error and freshness deadline, but not UpdatedAt.
	// Checking the installation time prevents retries or repeated reads from extending old data's life.
	cached.value, cached.hasData, cached.expiresAt = nil, false, time.Time{}
	cached.staleReason = ReasonMaxAge
	c.advanceRevisionLocked(&cached)
	c.storeEntryLocked(key, cached)
	c.stats.AgeExpirations++
	if cached.err == nil {
		c.forgetCapacityLocked(key)
	}
	c.owner.emitEventLocked(Event{Key: key, Kind: EventCacheRemoved, Reason: ReasonMaxAge})
	return true
}
