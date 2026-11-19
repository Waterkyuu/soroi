package soroi

import "reflect"

// readSnapshot holds the result and its static binding for one published value version.
type readSnapshot struct {
	// typ distinguishes a valid typed nil from an incompatible requested type.
	typ reflect.Type
	// value shares the entry's immutable application data with concurrent readers.
	value any
}

// readPublished renews an indexed idle task without taking the bucket read lock.
// A missing, invalidated or retired publication sends the reader back to the locked entry.
func (c *clientBucket) readPublished(key any, typ reflect.Type) (any, bool) {
	if _, text := key.(string); !text && !reflect.ValueOf(key).Comparable() {
		return nil, false
	}
	indexed, ok := c.readTasks.Load(key)
	if !ok {
		return nil, false
	}
	task := indexed.(*gcTask)
	snapshot := task.snapshot.Load()
	if snapshot == nil || (typ != nil && typ != snapshot.typ) {
		return nil, false
	}
	if !task.recordAccess(c.owner.gcElapsed(task.startedAt)) || task.snapshot.Load() != snapshot {
		return nil, false
	}
	return snapshot.value, true
}

// publishReadLocked makes the first successful value read reusable until this entry changes.
// The bucket read lock prevents writers from invalidating a version during publication.
func (c *clientBucket) publishReadLocked(key any, cached *entry) {
	task := cached.gcTask
	if task == nil || c.owner.options.MaxAge > 0 {
		return
	}
	if task.snapshot.Load() == nil {
		task.snapshot.CompareAndSwap(nil, &readSnapshot{value: cached.value, typ: cached.typ})
	}
	if !task.indexed.Load() && task.indexed.CompareAndSwap(false, true) {
		c.readTasks.Store(key, task)
	}
}

// invalidateReadLocked drops a published version while the writer owns its bucket.
func (cached *entry) invalidateReadLocked() {
	if task := cached.gcTask; task != nil && task.snapshot.Load() != nil {
		task.snapshot.Store(nil)
	}
}
