package broker

import (
	"fmt"
	"testing"

	"vinr.eu/snapflux/internal/storage"
)

// makeBrokers creates a slice of N broker entities with deterministic IDs.
func makeBrokers(n int) []storage.BrokerEntity {
	b := make([]storage.BrokerEntity, n)
	for i := range n {
		b[i] = storage.BrokerEntity{ID: fmt.Sprintf("node-%d", i), Host: fmt.Sprintf("h%d", i), Address: fmt.Sprintf("http://h%d:8080", i)}
	}
	return b
}

// largeKeySet returns n distinct partition keys.
func largeKeySet(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("topic/key-%04d", i)
	}
	return keys
}

func TestHashRingEmptyReturnsError(t *testing.T) {
	r := newHashRing(nil)
	if r.nodeCount() != 0 {
		t.Fatalf("expected 0 nodes, got %d", r.nodeCount())
	}
	_, err := r.getNode("any-key")
	if err == nil {
		t.Fatal("expected error from empty ring, got nil")
	}
}

func TestHashRingSingleNodeOwnsAllKeys(t *testing.T) {
	r := newHashRing(makeBrokers(1))
	for _, key := range largeKeySet(200) {
		node, err := r.getNode(key)
		if err != nil {
			t.Fatalf("getNode(%q): %v", key, err)
		}
		if node.ID != "node-0" {
			t.Errorf("key %q: expected node-0, got %s", key, node.ID)
		}
	}
}

// TestHashRingDeterminism: ring built from the same broker set in reversed order
// must produce identical assignments for all keys.
func TestHashRingDeterminism(t *testing.T) {
	brokers := makeBrokers(5)
	reversed := make([]storage.BrokerEntity, len(brokers))
	copy(reversed, brokers)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}

	r1 := newHashRing(brokers)
	r2 := newHashRing(reversed)

	for _, key := range largeKeySet(500) {
		n1, _ := r1.getNode(key)
		n2, _ := r2.getNode(key)
		if n1.ID != n2.ID {
			t.Errorf("key %q: r1→%s r2→%s (ring order must not affect assignments)", key, n1.ID, n2.ID)
		}
	}
}

// TestHashRingNodeCountDeduplication: nodeCount reports unique broker IDs,
// not virtual node entries.
func TestHashRingNodeCountDeduplication(t *testing.T) {
	brokers := makeBrokers(4)
	r := newHashRing(brokers)
	if got := r.nodeCount(); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}
	// Ring has 4 × virtualNodes entries but nodeCount must still be 4.
	if len(r.ring) != 4*virtualNodes {
		t.Fatalf("expected %d ring entries, got %d", 4*virtualNodes, len(r.ring))
	}
}

// TestHashRingMinimalDisruption: removing one broker from a 5-node ring should
// remap only roughly 1/5 (≈20%) of keys, not all of them.
func TestHashRingMinimalDisruption(t *testing.T) {
	const total = 1000
	brokers := makeBrokers(5)
	before := newHashRing(brokers)

	// Remove node-2 from the set.
	reduced := append(brokers[:2:2], brokers[3:]...)
	after := newHashRing(reduced)

	keys := largeKeySet(total)
	changed := 0
	for _, key := range keys {
		b, _ := before.getNode(key)
		a, _ := after.getNode(key)
		if b.ID != a.ID {
			changed++
		}
	}

	pct := float64(changed) / total * 100
	// With 5 nodes, ~20% should remap. Allow [8%, 35%] to avoid flakiness.
	if pct < 8 || pct > 35 {
		t.Errorf("expected ~20%% of keys to remap after removing 1/5 brokers, got %.1f%% (%d/%d)", pct, changed, total)
	}
}

// TestHashRingKeysNotOwnedByRemovedNode: after a broker is removed, none of the
// surviving ring's keys should point at the dead node.
func TestHashRingNoOrphanedKeys(t *testing.T) {
	brokers := makeBrokers(5)
	reduced := append(brokers[:2:2], brokers[3:]...)
	after := newHashRing(reduced)

	aliveIDs := make(map[string]bool)
	for _, b := range reduced {
		aliveIDs[b.ID] = true
	}

	for _, key := range largeKeySet(500) {
		node, err := after.getNode(key)
		if err != nil {
			t.Fatalf("getNode(%q): %v", key, err)
		}
		if !aliveIDs[node.ID] {
			t.Errorf("key %q routed to removed broker %q", key, node.ID)
		}
	}
}

// TestHashKeyDeterminism: hashKey must always produce the same value for the same input.
func TestHashKeyDeterminism(t *testing.T) {
	inputs := []string{"", "a", "orders/partition-0", "very-long-key-with-unicode-αβγ"}
	for _, k := range inputs {
		h1 := hashKey(k)
		h2 := hashKey(k)
		if h1 != h2 {
			t.Errorf("hashKey(%q) is non-deterministic: %d vs %d", k, h1, h2)
		}
	}
}

// TestHashKeyDistinctInputs: two different keys should (almost always) have different hashes.
func TestHashKeyDistinctInputs(t *testing.T) {
	collisions := 0
	for i := range 1000 {
		if hashKey(fmt.Sprintf("key-%d", i)) == hashKey(fmt.Sprintf("key-%d", i+1)) {
			collisions++
		}
	}
	if collisions > 2 {
		t.Errorf("too many hash collisions on sequential keys: %d", collisions)
	}
}

// TestIsSelfEmptyRingFailsOpen: when no brokers exist the IsSelf method must
// return true (fail-open) so the lone node handles all traffic.
func TestIsSelfEmptyRingFailsOpen(t *testing.T) {
	b := &Broker{id: "solo", ring: newHashRing(nil)}
	for _, key := range largeKeySet(20) {
		if !b.IsSelf(key) {
			t.Errorf("IsSelf(%q) = false on empty ring, want true (fail-open)", key)
		}
	}
}

// TestIsSelfOwnership: IsSelf returns true only for keys that hash to this broker,
// and false for keys that hash to a peer.
func TestIsSelfOwnership(t *testing.T) {
	brokers := makeBrokers(3)
	r := newHashRing(brokers)

	for _, b := range brokers {
		node := &Broker{id: b.ID, ring: r}
		// Find a key owned by this broker and one definitely owned by someone else.
		var ownedKey, otherKey string
		for _, key := range largeKeySet(500) {
			owner, _ := r.getNode(key)
			if owner.ID == b.ID && ownedKey == "" {
				ownedKey = key
			}
			if owner.ID != b.ID && otherKey == "" {
				otherKey = key
			}
			if ownedKey != "" && otherKey != "" {
				break
			}
		}
		if ownedKey == "" || otherKey == "" {
			t.Fatalf("broker %s: could not find both an owned and a non-owned key", b.ID)
		}
		if !node.IsSelf(ownedKey) {
			t.Errorf("broker %s: IsSelf(%q) = false, want true", b.ID, ownedKey)
		}
		if node.IsSelf(otherKey) {
			t.Errorf("broker %s: IsSelf(%q) = true, want false", b.ID, otherKey)
		}
	}
}

// TestGetNodeDelegatesToRing: GetNode must return the same broker as the ring's getNode.
func TestGetNodeDelegatesToRing(t *testing.T) {
	brokers := makeBrokers(3)
	r := newHashRing(brokers)
	b := &Broker{ring: r}

	for _, key := range largeKeySet(100) {
		want, _ := r.getNode(key)
		got, err := b.GetNode(key)
		if err != nil {
			t.Fatalf("GetNode(%q): %v", key, err)
		}
		if got.ID != want.ID {
			t.Errorf("GetNode(%q) = %s, ring.getNode = %s", key, got.ID, want.ID)
		}
	}
}

// TestNodeCountZeroBeforeStart: NodeCount returns 0 when ring is nil (before Start).
func TestNodeCountZeroBeforeStart(t *testing.T) {
	b := &Broker{}
	if n := b.NodeCount(); n != 0 {
		t.Errorf("NodeCount on uninitialised broker = %d, want 0", n)
	}
}
