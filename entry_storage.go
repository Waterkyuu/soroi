package soroi

// entryLocked copies retained state while its caller owns the client lock.
func (c *Client) entryLocked(key any) entry {
	return c.entries[key]
}

// storeEntryLocked replaces retained state while its caller owns the client lock.
func (c *Client) storeEntryLocked(key any, value entry) {
	c.entries[key] = value
}
