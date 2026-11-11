package soroi

import "time"

// gcTask identifies one inactive key's retention deadline and owns its scheduled cleanup.
type gcTask struct {
	// expiresAt is the inactivity deadline measured by the client's clock.
	expiresAt time.Time
	// stop releases the timer when the key is reused or the client closes.
	stop func()
}

// scheduleGC schedules one cleanup callback without creating a permanent background worker.
func scheduleGC(delay time.Duration, cleanup func()) func() {
	timer := time.AfterFunc(delay, cleanup)
	return func() { timer.Stop() }
}

// touchGCLocked restarts retention and releases empty, unowned bindings when capacity limiting is enabled.
// Subscribed or loading keys suspend timed cleanup.
func (c *clientBucket) touchGCLocked(key any) {
	// Unbounded clients without inactivity cleanup have no retention metadata to update.
	if c.owner.options.GCTime <= 0 && c.owner.options.MaxEntries <= 0 {
		return
	}
	c.stopGCLocked(key)
	if c.owner.closed {
		return
	}
	cached := c.entryLocked(key)
	unowned := len(c.observers[key]) == 0 && c.pending[key] == nil
	empty := !cached.hasData && cached.err == nil
	releaseBinding := c.owner.options.MaxEntries > 0 && unowned && empty
	if releaseBinding {
		// An evicted key needs its type binding only while a handle or load owns it.
		// Release empty metadata when that owner leaves, even when timed GC is disabled.
		c.discardLocked(key, ReasonNone)
		return
	}
	if c.owner.options.GCTime <= 0 {
		return
	}
	if _, exists := c.entries[key]; !exists {
		return
	}
	if len(c.observers[key]) > 0 || c.pending[key] != nil {
		return
	}
	task := &gcTask{expiresAt: c.owner.options.Clock().Add(c.owner.options.GCTime)}
	// Inactive retention allocates its index only when a key first needs a timer.
	if c.gcTasks == nil {
		c.gcTasks = make(map[any]*gcTask)
	}
	c.gcTasks[key] = task
	task.stop = c.owner.gcAfterFunc(c.owner.options.GCTime, func() { c.collectGC(key, task) })
}

// collectGC deletes only the inactive state belonging to the current cleanup task.
func (c *clientBucket) collectGC(key any, task *gcTask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Stop cannot retract a callback that has already started. Comparing task
	// identities prevents that callback from deleting a reused or recreated key.
	if c.gcTasks[key] != task {
		return
	}
	if remaining := task.expiresAt.Sub(c.owner.options.Clock()); remaining > 0 {
		task.stop = c.owner.gcAfterFunc(remaining, func() { c.collectGC(key, task) })
		return
	}
	delete(c.gcTasks, key)
	if len(c.observers[key]) == 0 && c.pending[key] == nil {
		c.stats.GCCollections++
		c.discardLocked(key, ReasonInactive)
	}
}

// stopGCLocked detaches a key's cleanup before stopping its timer.
func (c *clientBucket) stopGCLocked(key any) {
	if task := c.gcTasks[key]; task != nil {
		delete(c.gcTasks, key)
		task.stop()
	}
}

// clearGCLocked releases every cleanup timer when cached state is discarded.
func (c *clientBucket) clearGCLocked() {
	for key := range c.gcTasks {
		c.stopGCLocked(key)
	}
}
