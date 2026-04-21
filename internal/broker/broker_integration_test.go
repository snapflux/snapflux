//go:build integration

package broker

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/storage/mongodb"
)

// testKeys is a fixed set of partition keys used across all assertions.
var testKeys = func() []string {
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("topic/partition-%02d", i)
	}
	return keys
}()

func startMongoStore(t *testing.T) storage.Provider {
	t.Helper()
	ctx := context.Background()

	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongo container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	p := mongodb.New()
	if err := p.Connect(ctx, uri); err != nil {
		t.Fatalf("connect to mongo: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(ctx) })

	return p
}

// ringFromStore builds a fresh HashRing from whatever brokers are currently in the store.
func ringFromStore(t *testing.T, store storage.Provider) *HashRing {
	t.Helper()
	brokers, err := store.GetActiveBrokers(context.Background())
	if err != nil {
		t.Fatalf("GetActiveBrokers: %v", err)
	}
	return newHashRing(brokers)
}

// assertSingleOwner verifies that for every key in testKeys, exactly one of the
// provided rings maps that key to exactly one distinct broker ID, and that all
// rings agree on which broker that is.
func assertSingleOwner(t *testing.T, rings map[string]*HashRing) {
	t.Helper()
	for _, key := range testKeys {
		ownersByRing := make(map[string]string, len(rings)) // ringID → ownerID
		for ringID, r := range rings {
			node, err := r.getNode(key)
			if err != nil {
				t.Errorf("key %q ring %q: getNode error: %v", key, ringID, err)
				continue
			}
			ownersByRing[ringID] = node.ID
		}

		// All rings must agree on the same owner.
		var agreedOwner string
		for ringID, ownerID := range ownersByRing {
			if agreedOwner == "" {
				agreedOwner = ownerID
				continue
			}
			if ownerID != agreedOwner {
				t.Errorf("key %q: ring %q disagrees — got owner %q, others have %q",
					key, ringID, ownerID, agreedOwner)
			}
		}
	}
}

// TestBrokerPartitionSingleOwner registers N brokers, then kills them one by one.
// At each stable membership state it asserts:
//  1. Every partition key maps to exactly one broker.
//  2. All live brokers agree on who owns each key (their rings are consistent).
func TestBrokerPartitionSingleOwner(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	const nodeCount = 5
	brokerIDs := make([]string, nodeCount)

	// Register all nodes.
	for i := range nodeCount {
		id := fmt.Sprintf("broker-%d", i)
		brokerIDs[i] = id
		if err := store.InitBroker(ctx, id, fmt.Sprintf("host-%d", i), fmt.Sprintf("http://host-%d:8080", i)); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	// Verify full cluster: build one ring per live broker and check consistency.
	t.Run("full cluster", func(t *testing.T) {
		rings := make(map[string]*HashRing, nodeCount)
		for _, id := range brokerIDs {
			rings[id] = ringFromStore(t, store)
		}
		if rings[brokerIDs[0]].nodeCount() != nodeCount {
			t.Fatalf("expected %d nodes in ring, got %d", nodeCount, rings[brokerIDs[0]].nodeCount())
		}
		assertSingleOwner(t, rings)
	})

	// Kill brokers one by one and re-check after each removal.
	for i, dyingID := range brokerIDs {
		remaining := brokerIDs[i+1:]
		label := fmt.Sprintf("after killing %s (%d nodes left)", dyingID, len(remaining))

		if err := store.RemoveBroker(ctx, dyingID); err != nil {
			t.Fatalf("RemoveBroker %s: %v", dyingID, err)
		}

		t.Run(label, func(t *testing.T) {
			if len(remaining) == 0 {
				// Last broker gone — ring must be empty, getNode should error gracefully.
				r := ringFromStore(t, store)
				if r.nodeCount() != 0 {
					t.Errorf("expected empty ring, got %d nodes", r.nodeCount())
				}
				_, err := r.getNode("any-key")
				if err == nil {
					t.Error("expected error from empty ring, got nil")
				}
				return
			}

			rings := make(map[string]*HashRing, len(remaining))
			for _, id := range remaining {
				rings[id] = ringFromStore(t, store)
			}
			if rings[remaining[0]].nodeCount() != len(remaining) {
				t.Fatalf("expected %d nodes in ring, got %d", len(remaining), rings[remaining[0]].nodeCount())
			}
			assertSingleOwner(t, rings)
		})
	}
}

// TestBrokerEvictStale checks that EvictStaleBrokers removes timed-out nodes
// and that a rebuilt ring no longer contains them.
func TestBrokerEvictStale(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	// Register 3 brokers with an artificially old heartbeat by inserting them
	// and then checking eviction with a future cutoff.
	ids := []string{"alpha", "beta", "gamma"}
	for _, id := range ids {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	// Evict everything older than 1 hour from now — all brokers were just registered
	// so their heartbeats are ~now; nothing should be evicted yet.
	evicted, err := store.EvictStaleBrokers(ctx, time.Now().Add(-time.Hour), "none")
	if err != nil {
		t.Fatalf("EvictStaleBrokers (no-op): %v", err)
	}
	if len(evicted) != 0 {
		t.Errorf("expected 0 evictions, got %d: %v", len(evicted), evicted)
	}

	// Evict with cutoff = future (everything is stale relative to a future time),
	// excluding "alpha" which acts as the surviving watcher.
	evicted, err = store.EvictStaleBrokers(ctx, time.Now().Add(time.Hour), "alpha")
	if err != nil {
		t.Fatalf("EvictStaleBrokers (evict all): %v", err)
	}
	if len(evicted) != 2 {
		t.Fatalf("expected 2 evictions (beta + gamma), got %d: %v", len(evicted), evicted)
	}

	// Ring rebuilt from remaining state must contain only alpha.
	ring := ringFromStore(t, store)
	if ring.nodeCount() != 1 {
		t.Fatalf("expected 1 node after eviction, got %d", ring.nodeCount())
	}

	// Every key must now be owned by alpha.
	for _, key := range testKeys {
		node, err := ring.getNode(key)
		if err != nil {
			t.Fatalf("getNode(%q): %v", key, err)
		}
		if node.ID != "alpha" {
			t.Errorf("key %q: expected owner alpha, got %s", key, node.ID)
		}
	}
}

// TestInitBrokerIdempotent: calling InitBroker twice with the same ID must be a
// no-op upsert — the ring must not grow, and GetActiveBrokers must not return duplicates.
func TestInitBrokerIdempotent(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	for range 3 {
		if err := store.InitBroker(ctx, "singleton", "h1", "http://h1:8080"); err != nil {
			t.Fatalf("InitBroker: %v", err)
		}
	}

	brokers, err := store.GetActiveBrokers(ctx)
	if err != nil {
		t.Fatalf("GetActiveBrokers: %v", err)
	}
	if len(brokers) != 1 {
		t.Errorf("expected 1 broker after 3 upserts, got %d", len(brokers))
	}
	if brokers[0].ID != "singleton" {
		t.Errorf("unexpected broker ID %q", brokers[0].ID)
	}
}

// TestGetActiveBrokersEmpty: querying an empty store must return an empty slice, not an error.
func TestGetActiveBrokersEmpty(t *testing.T) {
	store := startMongoStore(t)
	brokers, err := store.GetActiveBrokers(context.Background())
	if err != nil {
		t.Fatalf("GetActiveBrokers on empty store: %v", err)
	}
	if len(brokers) != 0 {
		t.Errorf("expected 0 brokers, got %d", len(brokers))
	}
}

// TestEvictStaleWatcherNeverEvicted: the broker acting as the watcher (excludeID)
// must never be evicted from the store, even when its heartbeat is older than the cutoff.
func TestEvictStaleWatcherNeverEvicted(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	for _, id := range []string{"watcher", "peer-a", "peer-b"} {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	// Evict everyone whose heartbeat is older than a future timestamp — that
	// is every broker just registered — but exclude the watcher.
	evicted, err := store.EvictStaleBrokers(ctx, time.Now().Add(time.Hour), "watcher")
	if err != nil {
		t.Fatalf("EvictStaleBrokers: %v", err)
	}

	for _, id := range evicted {
		if id == "watcher" {
			t.Errorf("watcher was evicted despite being the excludeID")
		}
	}

	// Watcher must still be present in the store.
	brokers, err := store.GetActiveBrokers(ctx)
	if err != nil {
		t.Fatalf("GetActiveBrokers: %v", err)
	}
	found := false
	for _, b := range brokers {
		if b.ID == "watcher" {
			found = true
		}
	}
	if !found {
		t.Error("watcher is missing from GetActiveBrokers after eviction run")
	}
}

// TestEvictAndRecomputeStableIsNoOp: when no evictions occur and the broker count
// is unchanged, evictAndRecompute must not rebuild the ring.
func TestEvictAndRecomputeStableIsNoOp(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	ids := []string{"a", "b", "c"}
	for _, id := range ids {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	brokers, _ := store.GetActiveBrokers(ctx)
	b := New(store, &config.Config{HeartbeatTimeoutMs: 60_000}) // 60 s timeout — nothing stale
	b.id = "a"
	b.ring = newHashRing(brokers)

	ringBefore := b.ring
	b.evictAndRecompute()

	if b.ring != ringBefore {
		t.Error("ring was rebuilt despite no membership change")
	}
}

// TestEvictAndRecomputePicksUpNewBroker: when a new broker joins after the ring
// was last built, evictAndRecompute must detect the count change and rebuild.
func TestEvictAndRecomputePicksUpNewBroker(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	for _, id := range []string{"a", "b"} {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	brokers, _ := store.GetActiveBrokers(ctx)
	b := New(store, &config.Config{HeartbeatTimeoutMs: 60_000})
	b.id = "a"
	b.ring = newHashRing(brokers) // ring has 2 nodes

	// A new broker joins.
	if err := store.InitBroker(ctx, "c", "c", "http://c:8080"); err != nil {
		t.Fatalf("InitBroker c: %v", err)
	}

	b.evictAndRecompute()

	if b.ring.nodeCount() != 3 {
		t.Errorf("expected ring to have 3 nodes after new broker joined, got %d", b.ring.nodeCount())
	}
}

// TestEvictAndRecomputeRemovesDeadNode: when a broker's heartbeat goes stale,
// evictAndRecompute must evict it and rebuild the ring without it.
func TestEvictAndRecomputeRemovesDeadNode(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	for _, id := range []string{"live", "dead"} {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	brokers, _ := store.GetActiveBrokers(ctx)
	// Use a 0 ms timeout so all brokers are immediately considered stale,
	// but exclude "live" since it's the watcher.
	b := New(store, &config.Config{HeartbeatTimeoutMs: 0})
	b.id = "live"
	b.ring = newHashRing(brokers)

	b.evictAndRecompute()

	if b.ring.nodeCount() != 1 {
		t.Errorf("expected 1 node after dead broker evicted, got %d", b.ring.nodeCount())
	}
	node, err := b.ring.getNode("any-key")
	if err != nil {
		t.Fatalf("getNode after eviction: %v", err)
	}
	if node.ID != "live" {
		t.Errorf("remaining node should be 'live', got %q", node.ID)
	}
}

// TestHeartbeatUpdatesTimestamp: UpdateHeartbeat must refresh lastHeartbeat so
// the broker is not evicted by a subsequent EvictStaleBrokers call.
func TestHeartbeatUpdatesTimestamp(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	if err := store.InitBroker(ctx, "node", "h", "http://h:8080"); err != nil {
		t.Fatalf("InitBroker: %v", err)
	}

	// Update heartbeat, then evict with a past cutoff — node should survive.
	if err := store.UpdateHeartbeat(ctx, "node"); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	evicted, err := store.EvictStaleBrokers(ctx, time.Now().Add(-time.Hour), "nobody")
	if err != nil {
		t.Fatalf("EvictStaleBrokers: %v", err)
	}
	for _, id := range evicted {
		if id == "node" {
			t.Error("node was evicted despite a fresh heartbeat")
		}
	}
}

// TestOwnershipRingSymmetry: for every key in the large set, the broker that
// GetNode returns agrees that IsSelf is true for that key and false for all others.
func TestOwnershipRingSymmetry(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	const n = 4
	for i := range n {
		id := fmt.Sprintf("sym-%d", i)
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	brokers, _ := store.GetActiveBrokers(ctx)
	ring := newHashRing(brokers)

	// Build one Broker per node.
	nodes := make([]*Broker, n)
	for i, b := range brokers {
		nodes[i] = &Broker{id: b.ID, ring: ring}
	}

	keys := largeKeySet(300)
	for _, key := range keys {
		owner, err := ring.getNode(key)
		if err != nil {
			t.Fatalf("getNode: %v", err)
		}

		trueCount := 0
		for _, node := range nodes {
			if node.IsSelf(key) {
				trueCount++
				if node.id != owner.ID {
					t.Errorf("key %q: node %q claims ownership but ring says %q", key, node.id, owner.ID)
				}
			}
		}
		if trueCount != 1 {
			t.Errorf("key %q: %d nodes claim ownership, want exactly 1", key, trueCount)
		}
	}
}

// TestPartitionDistributionBalance: with enough keys, each broker should own
// a roughly equal share. No broker should be idle or monopolise the keyspace.
func TestPartitionDistributionBalance(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	const n = 5
	for i := range n {
		id := fmt.Sprintf("bal-%d", i)
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	ring := ringFromStore(t, store)
	counts := make(map[string]int, n)
	keys := largeKeySet(5000)
	for _, key := range keys {
		node, _ := ring.getNode(key)
		counts[node.ID]++
	}

	if len(counts) != n {
		t.Fatalf("expected %d distinct owners across 5000 keys, got %d", n, len(counts))
	}

	// Ideal share is 1/n. Fail if any broker holds less than 5% or more than 35%.
	total := len(keys)
	for id, c := range counts {
		pct := float64(c) / float64(total) * 100
		if pct < 5 || pct > 35 {
			t.Errorf("broker %s owns %.1f%% of keys (%d/%d) — distribution is unbalanced", id, pct, c, total)
		}
	}
}

// TestRingConsistencyAcrossIndependentBuilds: two brokers that independently
// fetch membership from the same store and build their own rings must agree on
// every key's owner — even across different topology changes.
func TestRingConsistencyAcrossIndependentBuilds(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	phases := [][]string{
		{"x", "y", "z"},
		{"x", "y", "z", "w"},
		{"x", "z", "w"},
		{"z", "w"},
	}

	for _, phase := range phases {
		// Wipe and re-register only the brokers for this phase.
		current, _ := store.GetActiveBrokers(ctx)
		for _, b := range current {
			_ = store.RemoveBroker(ctx, b.ID)
		}
		for _, id := range phase {
			_ = store.InitBroker(ctx, id, id, "http://"+id+":8080")
		}

		// Each live broker independently builds its ring.
		rings := make(map[string]*HashRing, len(phase))
		for _, id := range phase {
			rings[id] = ringFromStore(t, store)
		}

		assertSingleOwner(t, rings)

		// Verify ring sizes match the phase membership exactly.
		for id, r := range rings {
			if r.nodeCount() != len(phase) {
				t.Errorf("phase %v: ring for %s has %d nodes, want %d", phase, id, r.nodeCount(), len(phase))
			}
		}
	}
}

// TestRemoveBrokerThenReAdd: removing a broker and re-registering it (e.g. restart)
// must restore it to the ring with the same ownership semantics.
func TestRemoveBrokerThenReAdd(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	for _, id := range []string{"p", "q", "r"} {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	ringBefore := ringFromStore(t, store)

	// Kill and revive "q".
	if err := store.RemoveBroker(ctx, "q"); err != nil {
		t.Fatalf("RemoveBroker: %v", err)
	}
	if err := store.InitBroker(ctx, "q", "q", "http://q:8080"); err != nil {
		t.Fatalf("re-InitBroker: %v", err)
	}

	ringAfter := ringFromStore(t, store)

	if ringAfter.nodeCount() != 3 {
		t.Fatalf("expected 3 nodes after re-add, got %d", ringAfter.nodeCount())
	}

	// Ownership must be identical to before (same membership → same ring).
	for _, key := range testKeys {
		before, _ := ringBefore.getNode(key)
		after, _ := ringAfter.getNode(key)
		if before.ID != after.ID {
			t.Errorf("key %q: owner changed from %s to %s after remove+re-add", key, before.ID, after.ID)
		}
	}
}

// TestEvictedBrokersReturnedByEvictStaleBrokers: the return value of
// EvictStaleBrokers must list exactly the IDs that were removed.
func TestEvictedBrokersReturnedByEvictStaleBrokers(t *testing.T) {
	ctx := context.Background()
	store := startMongoStore(t)

	all := []string{"keep", "drop-1", "drop-2", "drop-3"}
	for _, id := range all {
		if err := store.InitBroker(ctx, id, id, "http://"+id+":8080"); err != nil {
			t.Fatalf("InitBroker %s: %v", id, err)
		}
	}

	evicted, err := store.EvictStaleBrokers(ctx, time.Now().Add(time.Hour), "keep")
	if err != nil {
		t.Fatalf("EvictStaleBrokers: %v", err)
	}

	if len(evicted) != 3 {
		t.Fatalf("expected 3 evictions, got %d: %v", len(evicted), evicted)
	}

	sort.Strings(evicted)
	want := []string{"drop-1", "drop-2", "drop-3"}
	for i, id := range want {
		if evicted[i] != id {
			t.Errorf("evicted[%d] = %q, want %q", i, evicted[i], id)
		}
	}
}
