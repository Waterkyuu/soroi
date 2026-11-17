package soroi

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGetRetainedValues verifies that value reads preserve zero and nil data and keep stale data available.
func TestGetRetainedValues(t *testing.T) {
	client := NewClient(Options{})
	t.Cleanup(client.Close)
	if err := Set(client, "zero", 0); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[int](client, "zero"); !ok || value != 0 {
		t.Fatalf("typed zero = (%v, %v)", value, ok)
	}
	if value, ok := client.Get("zero"); !ok || value != 0 {
		t.Fatalf("untyped zero = (%v, %v)", value, ok)
	}
	if state := GetState[int](client, "zero"); !state.Stale || !state.HasData {
		t.Fatalf("retained zero state = %+v", state)
	}
	if err := Set[*int](client, "nil", nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[*int](client, "nil"); !ok || value != nil {
		t.Fatalf("typed nil = (%v, %v)", value, ok)
	}
	if err := Set[any](client, "nil-interface", nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := client.Get("nil-interface"); !ok || value != nil {
		t.Fatalf("untyped nil = (%v, %v)", value, ok)
	}
}

// TestGetUnavailableValues reports unreadable keys without exposing a value or binding a missing key.
func TestGetUnavailableValues(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	if err := Set(client, "typed", 42); err != nil {
		t.Fatal(err)
	}
	if value, ok := Get[string](client, "typed"); ok || value != "" {
		t.Fatalf("type mismatch = (%v, %v)", value, ok)
	}
	for _, key := range []any{"missing", nil, []int{1}} {
		if value, ok := Get[int](client, key); ok || value != 0 {
			t.Fatalf("typed read of unavailable key %T = (%v, %v)", key, value, ok)
		}
		if value, ok := client.Get(key); ok || value != nil {
			t.Fatalf("untyped read of unavailable key %T = (%v, %v)", key, value, ok)
		}
	}
	if err := Set(client, "missing", "now present"); err != nil {
		t.Fatalf("reading a missing key bound its type: %v", err)
	}
	client.Close()
	if value, ok := Get[int](client, "typed"); ok || value != 0 {
		t.Fatalf("closed read = (%v, %v)", value, ok)
	}
}

// TestGetSkipsFreshnessClock verifies that ordinary value reads avoid freshness calculation.
func TestGetSkipsFreshnessClock(t *testing.T) {
	calls := 0
	client := NewClient(Options{
		StaleTime: time.Hour,
		Clock: func() time.Time {
			calls++
			return time.Unix(0, 0)
		},
	})
	t.Cleanup(client.Close)
	if err := Set(client, "key", "value"); err != nil {
		t.Fatal(err)
	}
	before := calls
	if value, ok := Get[string](client, "key"); !ok || value != "value" {
		t.Fatalf("typed read = (%v, %v)", value, ok)
	}
	if value, ok := client.Get("key"); !ok || value != "value" {
		t.Fatalf("untyped read = (%v, %v)", value, ok)
	}
	if calls != before {
		t.Fatalf("Get called the clock %d times", calls-before)
	}
}

// TestGetTracksRecency verifies that value reads participate in capacity eviction.
func TestGetTracksRecency(t *testing.T) {
	client := NewClient(Options{MaxEntries: 2})
	t.Cleanup(client.Close)
	for _, key := range []string{"a", "b"} {
		if err := Set(client, key, key); err != nil {
			t.Fatal(err)
		}
	}
	if value, ok := Get[string](client, "a"); !ok || value != "a" {
		t.Fatalf("pre-eviction read = (%v, %v)", value, ok)
	}
	if err := Set(client, "c", "c"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.Get("b"); ok {
		t.Fatal("the least recently read value survived eviction")
	}
	if _, ok := client.Get("a"); !ok {
		t.Fatal("the last value read was evicted")
	}
}

// TestGetMaxAgeSharesLiveReads keeps a live value readable while its bucket has another reader.
func TestGetMaxAgeSharesLiveReads(t *testing.T) {
	client, now := newAgeClient(t, Options{MaxAge: time.Minute})
	// The atomic test clock can use the same read-lock path as the default clock.
	client.concurrentClock = true
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(time.Minute - time.Nanosecond))

	bucket := client.bucketFor("key")
	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	done := make(chan bool, 1)
	go func() {
		typed, typedOK := Get[int](client, "key")
		untyped, untypedOK := client.Get("key")
		done <- typedOK && typed == 42 && untypedOK && untyped == 42
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("a live value was unavailable just before MaxAge")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reading a live value waited for an existing reader")
	}
}

// TestGetMaxAgeConcurrentCleanup verifies that concurrent expired reads remove and publish once.
func TestGetMaxAgeConcurrentCleanup(t *testing.T) {
	client, now := newAgeClient(t, Options{MaxAge: time.Minute})
	client.concurrentClock = true
	if err := Set(client, "key", 42); err != nil {
		t.Fatal(err)
	}
	handle := Query(client, "key", func(context.Context) (int, error) { return 0, nil }, QueryOptions{Disable: true})
	t.Cleanup(handle.Close)
	<-handle.Updates()
	now.Store(int64(time.Minute))

	start := make(chan struct{})
	done := make(chan bool, 8)
	for range cap(done) {
		go func() {
			<-start
			value, ok := Get[int](client, "key")
			done <- !ok && value == 0
		}()
	}
	close(start)
	for range cap(done) {
		if !<-done {
			t.Error("Get returned an over-age value")
		}
	}
	if state := client.Stats(); state.AgeExpirations != 1 || state.CacheEntries != 0 {
		t.Fatalf("expiry stats = %+v", state)
	}
	select {
	case state := <-handle.Updates():
		if state.HasData || state.Fetching {
			t.Fatalf("expiry notification = %+v", state)
		}
	default:
		t.Fatal("expired value removal did not notify the handle")
	}
}

// TestGetMaxAgeRechecksReplacement keeps a write that wins between the read check and cleanup.
func TestGetMaxAgeRechecksReplacement(t *testing.T) {
	var now atomic.Int64
	var pause atomic.Bool
	checking := make(chan struct{})
	resume := make(chan struct{})
	releaseCheck := sync.OnceFunc(func() { close(resume) })
	defer releaseCheck()

	client := NewClient(Options{
		MaxAge: time.Minute,
		Clock: func() time.Time {
			at := time.Unix(0, now.Load())
			if pause.CompareAndSwap(true, false) {
				close(checking)
				<-resume
			}
			return at
		},
	})
	t.Cleanup(client.Close)
	client.concurrentClock = true
	if err := Set(client, "key", "old"); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(time.Minute))
	pause.Store(true)

	read := make(chan bool, 1)
	go func() {
		value, ok := Get[string](client, "key")
		read <- ok && value == "replacement"
	}()
	select {
	case <-checking:
	case <-time.After(3 * time.Second):
		t.Fatal("Get did not check the age of the old value")
	}

	write := make(chan error, 1)
	go func() { write <- Set(client, "key", "replacement") }()

	// The age check holds a read lock. A failed TryRLock means Set is waiting
	// for that reader and will acquire the writer lock before the cleanup can.
	bucket := client.bucketFor("key")
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for bucket.mu.raw.TryRLock() {
		bucket.mu.raw.RUnlock()
		select {
		case <-deadline.C:
			t.Fatal("Set did not queue behind the age check")
		default:
			runtime.Gosched()
		}
	}
	releaseCheck()
	if err := <-write; err != nil {
		t.Fatal(err)
	}
	if !<-read || client.Stats().AgeExpirations != 0 {
		t.Fatal("age cleanup discarded or missed the replacement")
	}
}
