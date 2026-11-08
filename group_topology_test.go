package soroi

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// assertGroupTopology compares optimized lock components with a full scan of live admitted memberships.
func assertGroupTopology(t *testing.T, c *Client) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	parents := make([]int, len(c.buckets))
	refs := make([]uint64, len(c.buckets))
	for i := range parents {
		parents[i] = i
	}
	root := func(i int) int {
		for parents[i] != i {
			i = parents[i]
		}
		return i
	}
	for group := range c.groups {
		first := -1
		for _, member := range group.members {
			if member.err != nil {
				continue
			}
			index := member.bucket.index
			refs[index]++
			if first < 0 {
				first = index
			} else {
				parents[root(index)] = root(first)
			}
		}
	}
	for i := range c.buckets {
		bucket := &c.buckets[i]
		if bucket.groupRefs != refs[i] {
			t.Fatalf("membership count mismatch: bucket=%d got=%d want=%d", i, bucket.groupRefs, refs[i])
		}
		var want []*clientBucket
		for j := range c.buckets {
			if refs[j] > 0 && root(j) == root(i) {
				want = append(want, &c.buckets[j])
			}
		}
		if len(want) <= 1 {
			if bucket.groupLocks != nil {
				t.Fatalf("lock component mismatch: singleton bucket %d retained a set", i)
			}
			continue
		}
		set := bucket.groupLocks
		if set == nil || len(set.buckets) != len(want) {
			t.Fatalf("lock component mismatch: bucket=%d want=%d members", i, len(want))
		}
		for j, member := range want {
			if set.buckets[j] != member || member.groupLocks != set {
				t.Fatalf("lock component mismatch: unordered or inconsistent set at bucket=%d", i)
			}
		}
	}
}

// TestGroupTopologyBridge checks redundant links, a real split, duplicate members, and independent progress.
func TestGroupTopologyBridge(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	keys := groupScopeKeys(t, c, 6)
	left := groupScopeSubscribe(t, c, keys[0], keys[1], keys[0])
	right := groupScopeSubscribe(t, c, keys[2], keys[3])
	other := groupScopeSubscribe(t, c, keys[4], keys[5])
	assertGroupTopology(t, c)
	bridge := groupScopeSubscribe(t, c, keys[1], keys[2])
	duplicate := groupScopeSubscribe(t, c, keys[1], keys[2])
	assertGroupTopology(t, c)
	bridge.Close()
	assertGroupTopology(t, c)
	duplicate.Close()
	assertGroupTopology(t, c)
	groupScopeWriteWhileBlocked(t, c, keys[0], keys[2])
	groupScopeWriteWhileBlocked(t, c, keys[2], keys[4])
	left.Close()
	left.Close()
	right.Close()
	other.Close()
	assertGroupTopology(t, c)
	c.Close()
	assertGroupTopology(t, c)
}

// TestGroupTopologySequence checks full-scan equivalence across bucket counts and repeated graph mutations.
func TestGroupTopologySequence(t *testing.T) {
	for _, count := range []int{1, 8, 64, 128} {
		t.Run(fmt.Sprintf("buckets=%d", count), func(t *testing.T) {
			c := newClient(Options{StaleTime: time.Hour}, count)
			t.Cleanup(c.Close)
			keys := groupScopeKeys(t, c, min(count, 16))
			var active []*QueriesHandle[[]Snapshot[any]]
			for step := range 120 {
				a, b := (step*7+1)%len(keys), (step*11+3)%len(keys)
				active = append(active, groupScopeSubscribe(t, c, keys[a], keys[b], keys[a]))
				assertGroupTopology(t, c)
				if len(active) > 8 {
					index := (step * 3) % len(active)
					active[index].Close()
					active = append(active[:index], active[index+1:]...)
					assertGroupTopology(t, c)
				}
			}
			c.Close()
			assertGroupTopology(t, c)
		})
	}
}

// TestGroupTopologyInvalidMembers keeps rejected definitions out of the synchronization graph.
func TestGroupTopologyInvalidMembers(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	keys := groupScopeKeys(t, c, 2)
	if err := Set(c, keys[1], 42); err != nil {
		t.Fatal(err)
	}
	group := Queries(
		c,
		nil,
		Definition[int]{
			Key:     keys[0],
			Fetcher: func(context.Context) (int, error) { return 0, nil },
			Options: QueryOptions{Disable: true},
		},
		Definition[string]{
			Key:     keys[1],
			Fetcher: func(context.Context) (string, error) { return "", nil },
			Options: QueryOptions{Disable: true},
		},
	)
	states := <-group.Updates()
	if states[0].Err == nil || states[1].Err != nil || states[2].Err == nil {
		t.Fatalf("unexpected admitted members: %+v", states)
	}
	assertGroupTopology(t, c)
	group.Close()
	assertGroupTopology(t, c)
}

// TestGroupTopologyWideAdmission checks unions wider than one machine word and repeated component membership.
func TestGroupTopologyWideAdmission(t *testing.T) {
	for _, count := range []int{64, 128} {
		t.Run(fmt.Sprintf("buckets=%d", count), func(t *testing.T) {
			c := newClient(Options{StaleTime: time.Hour}, count)
			t.Cleanup(c.Close)
			keys := groupScopeKeys(t, c, count)
			var pairs []*QueriesHandle[[]Snapshot[any]]
			for i := 0; i < count; i += 2 {
				pairs = append(pairs, groupScopeSubscribe(t, c, keys[i], keys[i+1]))
			}
			assertGroupTopology(t, c)
			members := append(append([]string(nil), keys...), keys...)
			wide := groupScopeSubscribe(t, c, members...)
			assertGroupTopology(t, c)
			wide.Close()
			assertGroupTopology(t, c)
			for _, pair := range pairs {
				pair.Close()
			}
			assertGroupTopology(t, c)
		})
	}
}
