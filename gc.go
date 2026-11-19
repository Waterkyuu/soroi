package soroi

import (
	"sync/atomic"
	"time"
)

// retiredGCOffset prevents detached tasks from accepting further lock-free renewals.
const retiredGCOffset int64 = -1

// gcTask identifies one inactive key's retention deadline and owns its scheduled cleanup.
type gcTask struct {
	// startedAt retains the clock reading from the first idle access covered by this task.
	startedAt time.Time
	// accessOffset records the latest access as nanoseconds since startedAt.
	// Offsets preserve the monotonic component of the default clock.
	accessOffset atomic.Int64
	// snapshot holds the version first published by a successful Get.
	snapshot atomic.Pointer[readSnapshot]
	// indexed records membership in the bucket read index until exclusive detachment.
	indexed atomic.Bool
	// stop releases the current timer when the task is detached.
	stop func()
}

// recordAccess renews the idle window. A later reader can finish first, so
// publication only moves the latest access forward.
func (task *gcTask) recordAccess(elapsed time.Duration) bool {
	offset := int64(elapsed)
	for {
		previous := task.accessOffset.Load()
		if previous == retiredGCOffset {
			return false
		}
		if offset <= previous || task.accessOffset.CompareAndSwap(previous, offset) {
			return true
		}
	}
}

// gcElapsed samples the idle task's clock without reading wall time on default-clock hits.
func (c *Client) gcElapsed(startedAt time.Time) time.Duration {
	if c.gcDefaultClock {
		return time.Since(startedAt)
	}
	return c.options.Clock().Sub(startedAt)
}

// scheduleGC schedules one cleanup callback without creating a permanent background worker.
func scheduleGC(delay time.Duration, cleanup func()) func() {
	timer := time.AfterFunc(delay, cleanup)
	return func() { timer.Stop() }
}

// touchGCReadLocked records an access under the bucket read lock. It returns
// false when an idle record needs a new task under exclusive protection.
// The caller uses the default concurrent clock and holds a live entry.
func (c *clientBucket) touchGCReadLocked(key any, cached *entry) bool {
	if c.owner.options.GCTime <= 0 {
		return true
	}
	if task := cached.gcTask; task != nil {
		task.recordAccess(c.owner.gcElapsed(task.startedAt))
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
	cached := c.entries[key]
	if c.owner.closed {
		c.stopIdleTaskLocked(key, cached)
		return
	}
	unowned := len(c.observers[key]) == 0 && c.pending[key] == nil
	empty := cached == nil || (!cached.hasData && cached.err == nil)
	releaseBinding := options.MaxEntries > 0 && unowned && empty
	if releaseBinding {
		// An evicted key needs its type binding only while a handle or load owns it.
		// Release empty metadata when that owner leaves, even when timed GC is disabled.
		c.discardLocked(key, ReasonNone)
		return
	}
	if options.GCTime <= 0 || cached == nil || !unowned {
		c.stopIdleTaskLocked(key, cached)
		return
	}
	if task := cached.gcTask; task != nil && c.owner.concurrentClock {
		// The timer can wake at its old deadline and recheck this renewed window.
		task.recordAccess(c.owner.gcElapsed(task.startedAt))
		return
	}

	// Supplied clocks may move backwards. Preserve their serialized timer resets.
	c.stopIdleTaskLocked(key, cached)
	task := &gcTask{startedAt: options.Clock()}
	cached.gcTask = task
	task.stop = c.owner.gcAfterFunc(options.GCTime, func() { c.collectGC(key, task) })
}

// collectGC deletes only the inactive state belonging to the current cleanup task.
func (c *clientBucket) collectGC(key any, task *gcTask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Stop cannot retract a callback that has already started. Comparing task
	// identities prevents that callback from deleting a key with a different task.
	cached := c.entries[key]
	if cached == nil || cached.gcTask != task {
		return
	}
	for {
		offset := task.accessOffset.Load()
		lastAccess := task.startedAt.Add(time.Duration(offset))
		deadline := lastAccess.Add(c.owner.options.GCTime)
		if remaining := deadline.Sub(c.owner.options.Clock()); remaining > 0 {
			task.stop = c.owner.gcAfterFunc(remaining, func() { c.collectGC(key, task) })
			return
		}
		// A reader can renew while this callback holds the bucket write lock.
		if task.accessOffset.CompareAndSwap(offset, retiredGCOffset) {
			break
		}
	}
	c.stopIdleTaskLocked(key, cached)
	if len(c.observers[key]) == 0 && c.pending[key] == nil {
		c.stats.GCCollections++
		c.discardLocked(key, ReasonInactive)
	}
}

// stopGCLocked detaches a key's cleanup before stopping its timer.
func (c *clientBucket) stopGCLocked(key any) {
	if c.owner.options.GCTime > 0 {
		c.stopIdleTaskLocked(key, c.entries[key])
	}
}

// stopIdleTaskLocked detaches a record's timer while holding its bucket exclusively.
func (c *clientBucket) stopIdleTaskLocked(key any, cached *entry) {
	if cached != nil && cached.gcTask != nil {
		task := cached.gcTask
		task.accessOffset.Store(retiredGCOffset)
		if task.indexed.Load() {
			c.readTasks.Delete(key)
		}
		cached.gcTask = nil
		task.stop()
	}
}

// clearGCLocked stops entry timers before their storage is cleared.
func (c *clientBucket) clearGCLocked() {
	if c.owner.options.GCTime > 0 {
		for key, cached := range c.entries {
			c.stopIdleTaskLocked(key, cached)
		}
	}
}
