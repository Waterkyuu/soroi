package soroi

import (
	"slices"
	"testing"
	"time"
)

// TestGroupAdmissionLockIdentity checks admission reuse while allowing the original full rebuild on removal.
func TestGroupAdmissionLockIdentity(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	keys := groupScopeKeys(t, c, 6)
	left := groupScopeSubscribe(t, c, keys[0], keys[1])
	other := groupScopeSubscribe(t, c, keys[4], keys[5])
	leftSet, otherSet := c.bucketFor(keys[0]).groupLocks, c.bucketFor(keys[4]).groupLocks
	oldMembers := slices.Clone(leftSet.buckets)
	duplicate := groupScopeSubscribe(t, c, keys[0], keys[1], keys[0])
	if c.bucketFor(keys[0]).groupLocks != leftSet || c.bucketFor(keys[4]).groupLocks != otherSet {
		t.Fatal("redundant admission replaced an unchanged lock set")
	}
	bridge := groupScopeSubscribe(t, c, keys[1], keys[2])
	if c.bucketFor(keys[4]).groupLocks != otherSet {
		t.Fatal("admission replaced an unrelated component")
	}
	assertGroupTopology(t, c)
	if !slices.Equal(leftSet.buckets, oldMembers) {
		t.Fatal("admission mutated a retired lock set")
	}
	bridge.Close()
	duplicate.Close()
	assertGroupTopology(t, c)
	if !slices.Equal(leftSet.buckets, oldMembers) {
		t.Fatal("removal mutated a retired lock set")
	}
	left.Close()
	other.Close()
	assertGroupTopology(t, c)
}
