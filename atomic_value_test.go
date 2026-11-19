package soroi

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAtomicGetBypassesBucketLock checks that eligible hits progress while the bucket is held exclusively.
func TestAtomicGetBypassesBucketLock(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the policy in the subtest output.
		name string
		// options selects the deadline checks exercised by each hit.
		options Options
	}{
		{name: "pure"},
		{name: "MaxAge", options: Options{MaxAge: time.Hour}},
		{name: "GCTime", options: Options{GCTime: time.Hour}},
		{name: "MaxAgeAndGCTime", options: Options{MaxAge: time.Hour, GCTime: time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(tc.options)
			t.Cleanup(client.Close)
			if err := Set(client, "key", 42); err != nil {
				t.Fatal(err)
			}
			bucket := client.bucketFor("key")
			bucket.mu.Lock()
			defer bucket.mu.Unlock()

			done := make(chan bool, 1)
			go func() {
				value, found := client.Get("key")
				typed, ok := Get[int](client, "key")
				done <- found && value == 42 && ok && typed == 42
			}()
			select {
			case ok := <-done:
				if !ok {
					t.Fatal("Get missed a published value")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Get waited for the bucket lock")
			}
		})
	}
}

// TestAtomicGCReadRacingCollector verifies that a read ordered before retirement renews the exact deadline.
func TestAtomicGCReadRacingCollector(t *testing.T) {
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
		got, ok := Get[int](client, "key")
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

// TestAtomicReadRacingRemove rechecks the binding when the captured timer was detached.
func TestAtomicReadRacingRemove(t *testing.T) {
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
	original := scheduler.latest(t)
	scheduler.now.Add(int64(10 * time.Second))
	pause.Store(true)

	read := make(chan bool, 1)
	go func() {
		got, ok := Get[int](client, "key")
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
		if err := client.Remove("key"); err != nil {
			write <- err
			return
		}
		write <- Set(client, "key", 43)
	}()
	select {
	case err := <-write:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Remove and Set waited for a lock-free Get")
	}
	release()
	if !<-read {
		t.Fatal("Get used an entry with a retired timer")
	}
	replacement := scheduler.latest(t)
	original.callback()
	if !original.stopped.Load() || scheduler.latest(t) != replacement {
		t.Fatal("detached timer disturbed the replacement")
	}
}

// TestAtomicIdleReadRacingOverwrite checks that renewing a shared timer cannot return a superseded snapshot.
func TestAtomicIdleReadRacingOverwrite(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.concurrentClock = true
	reading := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var pause atomic.Bool
	client.options.Clock = func() time.Time {
		if pause.CompareAndSwap(true, false) {
			close(reading)
			<-resume
		}
		return time.Unix(0, scheduler.now.Load())
	}
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	task := scheduler.latest(t)
	pause.Store(true)
	done := make(chan bool, 1)
	go func() {
		value, ok := client.Get("key")
		done <- ok && value == 43
	}()
	select {
	case <-reading:
	case <-time.After(3 * time.Second):
		t.Fatal("Get did not reach the renewal clock")
	}
	// The overwrite keeps the timer but replaces the immutable record while Get is renewing.
	write := make(chan error, 1)
	go func() { write <- Set(client, "key", 43) }()
	select {
	case err := <-write:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Set waited for the reader")
	}
	scheduler.now.Add(int64(10 * time.Second))
	release()
	if !<-done || scheduler.latest(t) != task {
		t.Fatal("Get must return the current value and retain the same timer")
	}
	scheduler.now.Add(int64(50 * time.Second))
	task.callback()
	if delay := scheduler.latest(t).delay; delay != 10*time.Second {
		t.Fatalf("remaining idle window = %v, want 10s", delay)
	}
}
