package soroi

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGCReadsReuseTimerAndKeepDeadline verifies that every read renews retention without resetting the timer.
func TestGCReadsReuseTimerAndKeepDeadline(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	// The atomic fixture clock can exercise the default clock's concurrent read path.
	client.concurrentClock = true
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	first := scheduler.latest(t)

	for range 2 {
		scheduler.now.Add(int64(20 * time.Second))
		if value, ok := Get[int](client, "key"); !ok || value != 42 {
			t.Fatalf("value read = (%v, %v)", value, ok)
		}
	}
	if scheduler.count() != 1 || first.stopped.Load() {
		t.Fatal("reading the key replaced its idle timer")
	}

	scheduler.now.Add(int64(20 * time.Second))
	first.callback()
	if delay := scheduler.latest(t).delay; delay != 40*time.Second {
		t.Fatalf("rescheduled delay = %v, want 40s", delay)
	}

	scheduler.now.Add(int64(40*time.Second - time.Nanosecond))
	scheduler.latest(t).callback()
	if delay := scheduler.latest(t).delay; delay != time.Nanosecond {
		t.Fatalf("rescheduled delay = %v, want 1ns", delay)
	}
	if stats := client.Stats(); stats.CacheEntries != 1 || stats.GCCollections != 0 {
		t.Fatalf("stats before expiry = %+v", stats)
	}
	scheduler.now.Add(1)
	scheduler.latest(t).callback()
	if _, ok := client.Get("key"); ok {
		t.Fatal("cached data survived its renewed deadline")
	}
	if count := client.Stats().GCCollections; count != 1 {
		t.Fatalf("cleanup count = %d, want 1", count)
	}
}

// TestGCConcurrentReadsKeepLatestAccess verifies that a later access wins even when its read returns first.
func TestGCConcurrentReadsKeepLatestAccess(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.concurrentClock = true
	atClock := make(chan struct{})
	resume := make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(resume) })
	defer releaseRead()
	var holdRead atomic.Bool
	client.options.Clock = func() time.Time {
		now := time.Unix(0, scheduler.now.Load())
		if holdRead.CompareAndSwap(true, false) {
			close(atClock)
			<-resume
		}
		return now
	}
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	first := scheduler.latest(t)
	scheduler.now.Add(int64(10 * time.Second))
	holdRead.Store(true)

	early := make(chan bool, 1)
	go func() {
		value, ok := Get[int](client, "key")
		early <- ok && value == 42
	}()
	select {
	case <-atClock:
	case <-time.After(3 * time.Second):
		t.Fatal("first Get did not reach the retention clock")
	}

	scheduler.now.Add(int64(10 * time.Second))
	late := make(chan bool, 1)
	go func() {
		value, ok := client.Get("key")
		late <- ok && value == 42
	}()
	select {
	case ok := <-late:
		if !ok {
			t.Fatal("later Get did not return the cached value")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("later Get waited for another reader")
	}
	releaseRead()
	if !<-early {
		t.Fatal("earlier Get did not return the cached value")
	}

	scheduler.now.Add(int64(40 * time.Second))
	first.callback()
	if delay := scheduler.latest(t).delay; delay != 20*time.Second {
		t.Fatalf("rescheduled delay = %v, want 20s from the later read", delay)
	}
}

// TestGCReusedTimerCannotRemoveReplacement guards key removal and recreation against an old callback.
func TestGCReusedTimerCannotRemoveReplacement(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.concurrentClock = true
	if err := Set(client, "key", "old"); err != nil {
		t.Fatal(err)
	}
	first := scheduler.latest(t)
	if err := client.Remove("key"); err != nil {
		t.Fatal(err)
	}
	scheduler.now.Add(int64(30 * time.Second))
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	second := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	first.callback()
	if !first.stopped.Load() || scheduler.latest(t) != second {
		t.Fatal("detached callback changed the replacement timer")
	}
	if stats := client.Stats(); stats.CacheEntries != 1 || stats.GCCollections != 0 {
		t.Fatalf("replacement stats = %+v", stats)
	}
	scheduler.now.Add(int64(30 * time.Second))
	second.callback()
	if _, ok := client.Get("key"); ok {
		t.Fatal("unused replacement survived its own deadline")
	}
}
