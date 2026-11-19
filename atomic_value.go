package soroi

import (
	"reflect"
	"sync/atomic"
	"time"
)

// valueSlot connects the existing lifecycle entry to its publicly readable result.
type valueSlot struct {
	// result publishes the immutable value and its type and age together.
	result atomic.Pointer[publishedValue]
}

// publishedValue contains the result fields needed by Get.
type publishedValue struct {
	// typ preserves the key's static result type.
	typ reflect.Type
	// value is the same immutable reference kept in the lifecycle entry.
	value any
	// updatedAt is the local installation time for calculating MaxAge.
	updatedAt time.Time
}

// publishesValues limits atomic reads to policies without serialized clock or capacity maintenance.
func (c *Client) publishesValues() bool {
	return c.concurrentClock && c.options.MaxEntries <= 0 && c.options.GCTime <= 0
}

// publishValueLocked updates only this key after changing its lifecycle entry under the bucket lock.
func (c *clientBucket) publishValueLocked(key any, cached *entry) {
	if !c.owner.publishesValues() {
		return
	}
	slot := cached.published
	isNew := slot == nil
	if isNew {
		slot = new(valueSlot)
		cached.published = slot
	}
	if cached.hasData {
		slot.result.Store(&publishedValue{typ: cached.typ, value: cached.value, updatedAt: cached.updatedAt})
	} else {
		slot.result.Store(nil)
	}
	if isNew {
		c.published.Store(key, slot)
	}
}

// getPublishedValue looks up a result without registering a bucket reader.
// done is false when age expiration needs the authoritative locked path.
func (c *clientBucket) getPublishedValue(key any, typ reflect.Type) (value any, ok, done bool) {
	if _, text := key.(string); !text && !reflect.ValueOf(key).Comparable() {
		return nil, false, true
	}
	indexed, found := c.published.Load(key)
	if !found {
		return nil, false, true
	}
	slot := indexed.(*valueSlot)
	result := slot.result.Load()
	if result == nil {
		return nil, false, true
	}
	if typ != nil && result.typ != nil && result.typ != typ {
		return nil, false, true
	}
	if age := c.owner.options.MaxAge; age > 0 && !c.owner.options.Clock().Before(result.updatedAt.Add(age)) {
		return nil, false, false
	}
	return result.value, true, true
}
