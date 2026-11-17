package soroi

import (
	"sync/atomic"
	"time"
)

// gcTask identifies one inactive key's retention deadline and owns its scheduled cleanup.
type gcTask struct {
	// startedAt retains the clock reading from the first idle access covered by this task.
	startedAt time.Time
	// accessOffset records the latest access as nanoseconds since startedAt.
	// Offsets preserve the monotonic component of the default clock.
	accessOffset atomic.Int64
	// stop releases the current timer when the task is detached.
	stop func()
}

// recordAccess renews the idle window. A later reader can finish first, so
// publication only moves the latest access forward.
func (task *gcTask) recordAccess(now time.Time) {
	offset := int64(now.Sub(task.startedAt))
	for previous := task.accessOffset.Load(); offset > previous; previous = task.accessOffset.Load() {
		if task.accessOffset.CompareAndSwap(previous, offset) {
			return
		}
	}
}

// scheduleGC schedules one cleanup callback without creating a permanent background worker.
func scheduleGC(delay time.Duration, cleanup func()) func() {
	timer := time.AfterFunc(delay, cleanup)
	return func() { timer.Stop() }
}

// touchGCReadLocked records an access under the bucket read lock. It returns
// false when an idle record needs a new task under exclusive protection.
// The caller uses the default concurrent clock and holds a live entry.
func (c *clientBucket) touchGCReadLocked(key any) bool {
	if c.owner.options.GCTime <= 0 {
		return true
	}
	if task := c.gcTasks[key]; task != nil {
		task.recordAccess(c.owner.options.Clock())
		return true
	}
	return len(c.observers[key]) > 0 || c.pending[key] != nil
}

// touchGCLocked renews retention and releases empty, unowned bindings when capacity limiting is enabled.
// Subscribed or loading keys suspend timed cleanup.
func (c *clientBucket) touchGCLocked(key any) {
	// Unbounded clients without inactivity cleanup have no retention metadata to update.
	options := &c.owner.options
	if options.GCTime <= 0 && options.MaxEntries <= 0 {
		return
	}
	if c.owner.closed {
		c.stopGCLocked(key)
		return
	}
	cached := c.entries[key]
	unowned := len(c.observers[key]) == 0 && c.pending[key] == nil
	empty := cached == nil || (!cached.hasData && cached.err == nil)
	releaseBinding := options.MaxEntries > 0 && unowned && empty
	if releaseBinding {
		// An evicted key needs its type binding only while a handle or load owns it.
		// Release empty metadata when that owner leaves, even when timed GC is disabled.
		c.stopGCLocked(key)
		c.discardLocked(key, ReasonNone)
		return
	}
	if options.GCTime <= 0 || cached == nil || !unowned {
		c.stopGCLocked(key)
		return
	}
	if task := c.gcTasks[key]; task != nil && c.owner.concurrentClock {
		// The timer can wake at its old deadline and recheck this renewed window.
		task.recordAccess(options.Clock())
		return
	}

	// Supplied clocks may move backwards. Preserve their serialized timer resets.
	c.stopGCLocked(key)
	task := &gcTask{startedAt: options.Clock()}
	// Inactive retention allocates its index only when a key first needs a timer.
	if c.gcTasks == nil {
		c.gcTasks = make(map[any]*gcTask)
	}
	c.gcTasks[key] = task
	task.stop = c.owner.gcAfterFunc(options.GCTime, func() { c.collectGC(key, task) })
}

// collectGC deletes only the inactive state belonging to the current cleanup task.
func (c *clientBucket) collectGC(key any, task *gcTask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Stop cannot retract a callback that has already started. Comparing task
	// identities prevents that callback from deleting a key with a different task.
	if c.gcTasks[key] != task {
		return
	}
	lastAccess := task.startedAt.Add(time.Duration(task.accessOffset.Load()))
	deadline := lastAccess.Add(c.owner.options.GCTime)
	if remaining := deadline.Sub(c.owner.options.Clock()); remaining > 0 {
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
