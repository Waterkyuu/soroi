package soroi

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

// differentBucketKeys finds two ordinary keys with distinct owners without depending on random seeds.
func differentBucketKeys(t *testing.T, client *Client) (string, string) {
	t.Helper()
	first := "bucket:first"
	for i := 0; i < 10000; i++ {
		other := fmt.Sprintf("bucket:%d", i)
		if client.bucketFor(first) != client.bucketFor(other) {
			return first, other
		}
	}
	t.Fatal("failed to find distinct buckets")
	return "", ""
}

// TestBucketIndependentWrites proves a blocked bucket does not serialize unrelated public API cycles.
func TestBucketIndependentWrites(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	first, other := differentBucketKeys(t, client)
	blocked := client.bucketFor(first)
	blocked.mu.Lock()
	locked := true
	defer func() {
		if locked {
			blocked.mu.Unlock()
		}
	}()
	firstDone := make(chan error, 1)
	go func() { firstDone <- Set(client, first, 1) }()
	otherDone := make(chan error, 1)
	go func() {
		if err := Set(client, other, 42); err != nil {
			otherDone <- err
			return
		}
		if state := Get[int](client, other); !state.HasData || state.Data != 42 {
			otherDone <- fmt.Errorf("read = %+v", state)
			return
		}
		if err := Update(client, other, func(value int) (int, error) { return value + 1, nil }); err != nil {
			otherDone <- err
			return
		}
		value, err := Fetch[int](context.Background(), client, other, nil)
		if err == nil && value != 43 {
			err = fmt.Errorf("fetch = %d", value)
		}
		otherDone <- err
	}()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("another bucket's write lock blocked this API cycle")
	}
	select {
	case err := <-firstDone:
		t.Fatalf("locked bucket completed early: %v", err)
	default:
	}
	blocked.mu.Unlock()
	locked = false
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if stats := client.Stats(); stats.CacheEntries != 2 || stats.CacheHits != 1 {
		t.Fatalf("global stats = %+v", stats)
	}
}

// TestBucketEqualKeys keeps Go equality, including signed zero and nested interface values, authoritative.
func TestBucketEqualKeys(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	pairs := [][2]any{
		{0.0, negativeZero},
		{complex(0.0, 0.0), complex(negativeZero, negativeZero)},
		{[2]float64{0, 1}, [2]float64{negativeZero, 1}},
		{struct{ Value any }{Value: 0.0}, struct{ Value any }{Value: negativeZero}},
	}
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	for i, pair := range pairs {
		if client.bucketFor(pair[0]) != client.bucketFor(pair[1]) {
			t.Fatalf("equal keys routed separately: %#v", pair)
		}
		if err := Set(client, pair[0], i+1); err != nil {
			t.Fatal(err)
		}
		if state := Get[int](client, pair[1]); !state.HasData || state.Data != i+1 {
			t.Fatalf("equivalent key lookup = %+v", state)
		}
	}
}

// TestBucketSingleOwner retains one Client lifecycle across every internal storage partition.
func TestBucketSingleOwner(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	for i := range client.buckets {
		if client.buckets[i].owner != client {
			t.Fatal("bucket created a separate Client owner")
		}
	}
	first, other := differentBucketKeys(t, client)
	Set(client, first, 1)
	Set(client, other, 2)
	client.Clear()
	if Get[int](client, first).HasData || Get[int](client, other).HasData || client.Stats().CacheEntries != 0 {
		t.Fatal("Clear left state in an internal bucket")
	}
	Set(client, first, 3)
	Set(client, other, 4)
	client.Close()
	if Get[int](client, first).Err != ErrClosed || Get[int](client, other).Err != ErrClosed {
		t.Fatal("Close did not close the entire Client")
	}
}

// TestBucketHandleReuse preserves saved ownership across state removal and coherent grouped updates.
func TestBucketHandleReuse(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	first, other := differentBucketKeys(t, client)
	fetch := func(context.Context) (int, error) { return 99, nil }
	handle := Query(client, first, fetch, QueryOptions{Disable: true})
	t.Cleanup(handle.Close)
	bucket := handle.bucket
	if bucket != client.bucketFor(first) {
		t.Fatal("handle retained the wrong bucket")
	}
	one, _ := QueryDefinition(first, fetch, QueryOptions{Disable: true})
	two, _ := QueryDefinition(other, fetch, QueryOptions{Disable: true})
	group := Queries(client, one, two)
	t.Cleanup(group.Close)
	for i, member := range group.group.members {
		if member.bucket != client.bucketFor(member.spec.key) {
			t.Fatalf("member %d retained wrong bucket", i)
		}
	}
	if err := Set(client, first, 1); err != nil {
		t.Fatal(err)
	}
	if err := Set(client, other, 2); err != nil {
		t.Fatal(err)
	}
	states := group.Snapshot()
	if states[0].Data != 1 || states[1].Data != 2 {
		t.Fatalf("group = %+v", states)
	}
	group.Close()
	client.mu.Lock()
	refs := bucket.groupRefs + client.bucketFor(other).groupRefs
	client.mu.Unlock()
	if refs != 0 {
		t.Fatalf("released group retained %d references", refs)
	}
	client.Clear()
	if handle.Snapshot().HasData {
		t.Fatal("saved owner retained cleared data")
	}
	value, err := handle.Refetch(context.Background())
	if err != nil || value != 99 || handle.bucket != bucket {
		t.Fatalf("saved owner after Clear: value=%d err=%v", value, err)
	}
	if err := client.Remove(first); err != nil {
		t.Fatal(err)
	}
	if err := Set(client, first, 7); err != nil {
		t.Fatal(err)
	}
	if state := handle.Snapshot(); !state.HasData || state.Data != 7 {
		t.Fatalf("saved owner after Remove: %+v", state)
	}
}

// TestBucketUpdateRecreation keeps conflict detection valid while the callback recreates the same key.
func TestBucketUpdateRecreation(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	key := "update:recreated"
	if err := Set(client, key, 1); err != nil {
		t.Fatal(err)
	}
	bucket := client.bucketFor(key)
	err := Update(client, key, func(value int) (int, error) {
		client.Clear()
		if err := Set(client, key, 10); err != nil {
			return 0, err
		}
		return value + 1, nil
	})
	if err != ErrUpdateConflict || Get[int](client, key).Data != 10 || client.bucketFor(key) != bucket {
		t.Fatalf("recreated state lost conflict or ownership: %v", err)
	}
}
