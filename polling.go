package soroi

import "time"

// pollTask owns one key's timer and the subscription supplying its interval and request policy.
type pollTask struct {
	// observer is the oldest enabled consumer with the shortest positive interval.
	observer *subscription
	// stop releases the scheduled callback when its owner changes or closes.
	stop func()
}

// schedulePoll arranges one interval callback without retaining a worker between intervals.
func schedulePoll(delay time.Duration, poll func()) func() {
	timer := time.AfterFunc(delay, poll)
	return func() { timer.Stop() }
}

// schedulePollingLocked retains one timer for the fastest enabled consumer of this key.
func (c *Client) schedulePollingLocked(key any) {
	var selected *subscription
	if !c.closed {
		for observer := range c.observers[key] {
			if !observer.enabled || observer.refetchInterval <= 0 {
				continue
			}
			if selected == nil {
				selected = observer
				continue
			}
			faster := observer.refetchInterval < selected.refetchInterval
			older := observer.refetchInterval == selected.refetchInterval && observer.order < selected.order
			if faster || older {
				selected = observer
			}
		}
	}
	if task := c.pollTasks[key]; task != nil && task.observer == selected {
		return
	}
	c.stopPollingLocked(key)
	if selected == nil {
		return
	}
	task := &pollTask{observer: selected}
	c.pollTasks[key] = task
	task.stop = c.pollAfterFunc(selected.refetchInterval, func() { c.poll(key, task) })
}

// poll starts a refresh only for the current schedule and skips occupied intervals without queuing work.
func (c *Client) poll(key any, task *pollTask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pollTasks[key] != task {
		return
	}
	delete(c.pollTasks, key)
	if c.pending[key] == nil {
		c.expireDataLocked(key)
		request := task.observer.request
		request.trigger = TriggerPoll
		c.startLocked(c.ctx, key, request)
	}
	c.schedulePollingLocked(key)
}

// stopPollingLocked detaches a schedule before stopping it so callbacks racing with Stop become harmless.
func (c *Client) stopPollingLocked(key any) {
	if task := c.pollTasks[key]; task != nil {
		delete(c.pollTasks, key)
		task.stop()
	}
}
