package soroi

import "slices"

// registerTagsLocked merges labels without replacing another consumer's labels or changing data revision.
func (c *clientBucket) registerTagsLocked(key any, tags []string) {
	if len(tags) == 0 {
		return
	}
	cached := c.entryLocked(key)
	for _, tag := range tags {
		if !slices.Contains(cached.tags, tag) {
			cached.tags = append(cached.tags, tag)
		}
	}
	c.storeEntryLocked(key, cached)
}

// matchesQueryTags accepts a query when any requested label is present in its registered labels.
func matchesQueryTags(registered, requested []string) bool {
	for _, tag := range requested {
		if slices.Contains(registered, tag) {
			return true
		}
	}
	return false
}
