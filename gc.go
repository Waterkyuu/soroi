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

// retiredGCOffset marks a task that can no longer accept renewals.
const retiredGCOffset = -1

// recordAccess renews this task unless removal or collection has retired it.
// A reader that ran later may already have published a newer time.
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
		cached.stopIdleTaskLocked()
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
		cached.stopIdleTaskLocked()
		return
	}
	if task := cached.gcTask; task != nil && c.owner.concurrentClock {
		// The timer can wake at its old deadline and recheck this renewed window.
		task.recordAccess(c.owner.gcElapsed(task.startedAt))
		return
	}

	// Supplied clocks may move backwards. Preserve their serialized timer resets.
	cached.stopIdleTaskLocked()
	task := &gcTask{startedAt: options.Clock()}
	cached.gcTask = task
	if cached.published != nil {
		cached.published.task.Store(task)
	}
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
		// A Get can renew the deadline even while the collector owns the bucket.
		if task.accessOffset.CompareAndSwap(offset, retiredGCOffset) {
			break
		}
	}
	cached.gcTask = nil
	if len(c.observers[key]) == 0 && c.pending[key] == nil {
		c.stats.GCCollections++
		c.discardLocked(key, ReasonInactive)
	}
}

// stopGCLocked detaches a key's cleanup before stopping its timer.
func (c *clientBucket) stopGCLocked(key any) {
	if c.owner.options.GCTime > 0 {
		c.entries[key].stopIdleTaskLocked()
	}
}

// stopIdleTaskLocked detaches a record's timer while holding its bucket exclusively.
func (cached *entry) stopIdleTaskLocked() {
	if cached != nil && cached.gcTask != nil {
		task := cached.gcTask
		task.accessOffset.Store(retiredGCOffset)
		if cached.published != nil {
			cached.published.task.Store(nil)
		}
		cached.gcTask = nil
		task.stop()
	}
}

// clearGCLocked stops entry timers before their storage is cleared.
func (c *clientBucket) clearGCLocked() {
	if c.owner.options.GCTime > 0 {
		for _, cached := range c.entries {
			cached.stopIdleTaskLocked()
		}
	}
}
