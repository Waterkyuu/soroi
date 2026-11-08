package soroi

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestUpdateReuseSameBucket preserves independence when another key advances the bucket revision counter.
func TestUpdateReuseSameBucket(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	if err := Set(c, "target", 100); err != nil {
		t.Fatal(err)
	}
	var other string
	for i := range 100000 {
		key := fmt.Sprintf("same-bucket:%d", i)
		if c.bucketFor(key) == c.bucketFor("target") {
			other = key
			break
		}
	}
	if other == "" {
		t.Fatal("could not find another key in the target bucket")
	}
	calls := 0
	err := Update(c, "target", func(value int) (int, error) {
		calls++
		if err := Set(c, other, 7); err != nil {
			return 0, err
		}
		return value + 10, nil
	})
	if err != nil || calls != 1 || Get[int](c, "target").Data != 110 || Get[int](c, other).Data != 7 {
		t.Fatalf("unrelated write rejected update: calls=%d error=%v", calls, err)
	}
}

// TestUpdateReuseTypeRebinding rejects writing through a stale node after a callback replaces its type.
func TestUpdateReuseTypeRebinding(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(fmt.Sprintf("clear=%t", clear), func(t *testing.T) {
			c := NewClient(Options{StaleTime: time.Hour})
			t.Cleanup(c.Close)
			if err := Set(c, "target", 100); err != nil {
				t.Fatal(err)
			}
			calls := 0
			err := Update(c, "target", func(value int) (int, error) {
				calls++
				if clear {
					c.Clear()
				} else if err := c.Remove("target"); err != nil {
					return 0, err
				}
				if err := Set(c, "target", "replacement"); err != nil {
					return 0, err
				}
				return value + 10, nil
			})
			if !errors.Is(err, ErrUpdateConflict) || calls != 1 || Get[string](c, "target").Data != "replacement" {
				t.Fatalf("rebound node was not preserved: calls=%d error=%v", calls, err)
			}
		})
	}
}

// TestUpdateReuseExpiredAdmission keeps maintenance and updater validation in their original order.
func TestUpdateReuseExpiredAdmission(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute, MaxEntries: capacity})
			if err := Set(c, "target", 100); err != nil {
				t.Fatal(err)
			}
			now.Add(int64(time.Minute))
			if err := Update[int](c, "target", nil); !errors.Is(err, ErrNoUpdater) {
				t.Fatal(err)
			}
			if c.Stats().AgeExpirations != 0 {
				t.Fatal("nil updater triggered maintenance")
			}
			calls := 0
			err := Update(c, "target", func(value int) (int, error) { calls++; return value + 1, nil })
			if !errors.Is(err, ErrNoData) || calls != 0 || c.Stats().AgeExpirations != 1 {
				t.Fatalf("expired admission: calls=%d error=%v stats=%+v", calls, err, c.Stats())
			}
		})
	}
}

// TestUpdateReuseNilAndFailure preserves nil data, callback error priority, and unlock-before-callback behavior.
func TestUpdateReuseNilAndFailure(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	if err := Set[any](c, "nil", nil); err != nil {
		t.Fatal(err)
	}
	if err := Update(c, "nil", func(value any) (any, error) {
		if value != nil {
			t.Fatalf("expected cached nil, got %v", value)
		}
		return 110, nil
	}); err != nil {
		t.Fatal(err)
	}
	if Get[any](c, "nil").Data != 110 {
		t.Fatal("nil data was treated as missing")
	}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_ = Update(c, "nil", func(any) (any, error) { panic("application failure") })
	}()
	if !panicked {
		t.Fatal("callback panic was suppressed")
	}
	if err := Set[any](c, "nil", 120); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("callback failure after close")
	err := Update(c, "nil", func(value any) (any, error) { c.Close(); return value, failure })
	if err != failure {
		t.Fatalf("callback error was replaced: %v", err)
	}
}
