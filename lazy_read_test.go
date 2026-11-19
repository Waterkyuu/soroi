package soroi

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLazyGCReadRacingCollector verifies that a read ordered before retirement renews the exact deadline.
func TestLazyGCReadRacingCollector(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.concurrentClock = true

	inClock := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var pause atomic.Bool
	client.options.Clock = func() time.Time {
		now := time.Unix(0, scheduler.now.Load())
		if pause.CompareAndSwap(true, false) {
			close(inClock)
			<-resume
		}
		return now
	}
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	if got, ok := client.Get("key"); !ok || got != 42 {
		t.Fatal("failed to warm key")
	}
	first := scheduler.latest(t)
	scheduler.now.Add(int64(time.Minute))
	pause.Store(true)

	collected := make(chan struct{})
	go func() {
		first.callback()
		close(collected)
	}()
	select {
	case <-inClock:
	case <-time.After(3 * time.Second):
		t.Fatal("collector did not reach the deadline check")
	}

	read := make(chan bool, 1)
	go func() {
		got, ok := client.Get("key")
		read <- ok && got == 42
	}()
	select {
	case ok := <-read:
		if !ok {
			t.Fatal("Get missed before the collector could retire the entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Get waited for the collector")
	}

	release()
	<-collected
	next := scheduler.latest(t)
	if next == first || next.delay != time.Minute {
		t.Fatalf("timer was not renewed for exactly one minute, delay = %v", next.delay)
	}
	scheduler.now.Add(int64(time.Minute - time.Nanosecond))
	next.callback()
	next = scheduler.latest(t)
	if next.delay != time.Nanosecond {
		t.Fatalf("delay = %v, want 1ns", next.delay)
	}
	scheduler.now.Add(1)
	next.callback()
	if _, ok := client.Get("key"); ok {
		t.Fatal("entry remained after its renewed deadline")
	}
}

// TestLazyReadRacingWrite rechecks a snapshot invalidated by an overwrite or by key recreation.
func TestLazyReadRacingWrite(t *testing.T) {
	t.Run("overwrite", func(t *testing.T) { testLazyReadRacingWrite(t, false) })
	t.Run("recreate", func(t *testing.T) { testLazyReadRacingWrite(t, true) })
}

// testLazyReadRacingWrite suspends a hot reader after lookup while the writer replaces its version.
func testLazyReadRacingWrite(t *testing.T, recreate bool) {
	client, scheduler := newGCClient(t, time.Minute)
	client.concurrentClock = true
	inClock := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var pause atomic.Bool
	client.options.Clock = func() time.Time {
		now := time.Unix(0, scheduler.now.Load())
		if pause.CompareAndSwap(true, false) {
			close(inClock)
			<-resume
		}
		return now
	}
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	if got, ok := client.Get("key"); !ok || got != 42 {
		t.Fatal("failed to warm key")
	}
	original := scheduler.latest(t)
	scheduler.now.Add(int64(10 * time.Second))
	pause.Store(true)

	read := make(chan bool, 1)
	go func() {
		got, ok := client.Get("key")
		read <- ok && got == 43
	}()
	select {
	case <-inClock:
	case <-time.After(3 * time.Second):
		t.Fatal("Get did not reach the renewal clock")
	}

	scheduler.now.Add(int64(10 * time.Second))
	write := make(chan error, 1)
	go func() {
		if recreate {
			if err := client.Remove("key"); err != nil {
				write <- err
				return
			}
		}
		write <- Set(client, "key", 43)
	}()
	select {
	case err := <-write:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer waited for the paused Get")
	}
	release()
	if !<-read {
		t.Fatal("Get returned a superseded publication")
	}
	replacement := scheduler.latest(t)
	if !recreate {
		if replacement != original {
			t.Fatal("overwrite replaced the idle task")
		}
		return
	}
	original.callback()
	if !original.stopped.Load() || scheduler.latest(t) != replacement {
		t.Fatal("detached timer disturbed the replacement")
	}
}

// TestLazyPublicationLifecycle checks first-read publication, overwrite invalidation and index ownership.
func TestLazyPublicationLifecycle(t *testing.T) {
	client := NewClient(Options{GCTime: time.Hour})
	t.Cleanup(client.Close)
	key := "key"
	bucket := client.bucketFor(key)
	for _, value := range []int{1, 2, 3} {
		if err := Set(client, key, value); err != nil {
			t.Fatal(err)
		}
		if _, ok := bucket.readTasks.Load(key); ok {
			t.Fatal("a write published an unread key")
		}
	}
	if got, ok := client.Get(key); !ok || got != 3 {
		t.Fatalf("first Get = %v, %v", got, ok)
	}
	found, ok := bucket.readTasks.Load(key)
	if !ok {
		t.Fatal("first Get did not publish")
	}
	task := found.(*gcTask)
	if err := Set(client, key, 4); err != nil {
		t.Fatal(err)
	}
	if task.snapshot.Load() != nil {
		t.Fatal("overwrite retained a snapshot")
	}
	if got, ok := client.Get(key); !ok || got != 4 {
		t.Fatalf("Get after Set = %v, %v", got, ok)
	}
	client.Clear()
	if _, ok := bucket.readTasks.Load(key); ok {
		t.Fatal("Clear retained a published task")
	}
	if got, ok := client.Get(key); ok || got != nil {
		t.Fatal("Get succeeded after Clear")
	}
}
