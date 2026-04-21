//go:build integration

package mongodb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/storage/mongodb"
)

func startStore(t *testing.T) storage.Provider {
	t.Helper()
	ctx := context.Background()

	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	p := mongodb.New()
	if err := p.Connect(ctx, uri); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(ctx) })
	return p
}

// TestMongoConcurrentReceiveNoDuplicates: multiple goroutines calling
// ReceiveDeliveries simultaneously must each receive distinct messages.
// Validates that FindOneAndUpdate is atomic under concurrent load.
func TestMongoConcurrentReceiveNoDuplicates(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	const messages = 30
	const consumers = 6

	// Subscribe so deliveries are created.
	if err := store.UpsertSubscription(ctx, "concurrent", "workers"); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	for i := range messages {
		id := fmt.Sprintf("msg-%03d", i)
		if err := store.SendMessage(ctx, id, "concurrent", fmt.Sprintf("k%d", i), i); err != nil {
			t.Fatalf("SendMessage %d: %v", i, err)
		}
		if err := store.CreateDeliveries(ctx, id, "concurrent", []string{"workers"}, 3); err != nil {
			t.Fatalf("CreateDeliveries %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deliveries, err := store.ReceiveDeliveries(ctx, "concurrent", "workers", messages, 30_000)
			if err != nil {
				t.Errorf("ReceiveDeliveries: %v", err)
				return
			}
			mu.Lock()
			for _, d := range deliveries {
				seen[d.ReceiptID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for receiptID, count := range seen {
		if count > 1 {
			t.Errorf("receipt %s delivered %d times (must be 1)", receiptID, count)
		}
	}
	if len(seen) != messages {
		t.Errorf("expected %d distinct deliveries, got %d", messages, len(seen))
	}
}

// TestMongoAckNonExistentReceiptReturnsFalse: acking an unknown receiptId must
// return (false, nil) without an error.
func TestMongoAckNonExistentReceiptReturnsFalse(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	ok, err := store.AckDelivery(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if ok {
		t.Error("acking a non-existent receipt must return false")
	}
}

// TestMongoDoubleAckReturnsFalse: the second ack on the same receiptId must
// return (false, nil) — the delivery was already deleted.
func TestMongoDoubleAckReturnsFalse(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	if err := store.UpsertSubscription(ctx, "acktest", "g"); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := store.SendMessage(ctx, "m1", "acktest", "k", "body"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := store.CreateDeliveries(ctx, "m1", "acktest", []string{"g"}, 3); err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}

	deliveries, err := store.ReceiveDeliveries(ctx, "acktest", "g", 1, 30_000)
	if err != nil || len(deliveries) == 0 {
		t.Fatalf("ReceiveDeliveries: err=%v count=%d", err, len(deliveries))
	}
	receiptID := deliveries[0].ReceiptID

	ok1, err := store.AckDelivery(ctx, receiptID)
	if err != nil || !ok1 {
		t.Fatalf("first Ack: err=%v ok=%v", err, ok1)
	}

	ok2, err := store.AckDelivery(ctx, receiptID)
	if err != nil {
		t.Fatalf("second Ack returned error: %v", err)
	}
	if ok2 {
		t.Error("second ack must return false (delivery already gone)")
	}
}

// TestMongoDeadLetterAfterMaxAttempts: deliveries that exhaust maxAttempts
// must be marked StatusDead and not returned by subsequent ReceiveDeliveries.
func TestMongoDeadLetterAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	if err := store.UpsertSubscription(ctx, "risky", "workers"); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := store.SendMessage(ctx, "m-risky", "risky", "k", "payload"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := store.CreateDeliveries(ctx, "m-risky", "risky", []string{"workers"}, 2); err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}

	// Receive twice without acking — exhaust attempts via RequeueStaleDeliveries.
	for attempt := range 2 {
		deliveries, err := store.ReceiveDeliveries(ctx, "risky", "workers", 1, 1) // 1ms visibility
		if err != nil || len(deliveries) == 0 {
			t.Fatalf("attempt %d: ReceiveDeliveries err=%v count=%d", attempt, err, len(deliveries))
		}
		// Let the visibility timeout expire.
		time.Sleep(10 * time.Millisecond)
		n, err := store.RequeueStaleDeliveries(ctx, time.Now())
		if err != nil {
			t.Fatalf("RequeueStaleDeliveries: %v", err)
		}
		t.Logf("attempt %d: requeued=%d", attempt, n)
	}

	// After 2 failed attempts (maxAttempts=2) the delivery should be dead.
	deliveries, err := store.ReceiveDeliveries(ctx, "risky", "workers", 10, 30_000)
	if err != nil {
		t.Fatalf("final ReceiveDeliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("dead-lettered message must not be redelivered, got %d", len(deliveries))
	}
}

// TestMongoRequeueRestoresVisibility: an inflight delivery whose visibility
// window has passed must be returned to StatusPending by RequeueStaleDeliveries.
func TestMongoRequeueRestoresVisibility(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	if err := store.UpsertSubscription(ctx, "requeue", "workers"); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := store.SendMessage(ctx, "m-req", "requeue", "k", "payload"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := store.CreateDeliveries(ctx, "m-req", "requeue", []string{"workers"}, 5); err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}

	// Receive with a 1ms visibility timeout.
	first, err := store.ReceiveDeliveries(ctx, "requeue", "workers", 1, 1)
	if err != nil || len(first) == 0 {
		t.Fatalf("first receive: err=%v count=%d", err, len(first))
	}

	// Immediately try to receive again — message is still inflight.
	second, _ := store.ReceiveDeliveries(ctx, "requeue", "workers", 1, 30_000)
	if len(second) != 0 {
		t.Error("message must not be double-delivered before visibility expires")
	}

	// Let visibility expire, then requeue.
	time.Sleep(10 * time.Millisecond)
	n, err := store.RequeueStaleDeliveries(ctx, time.Now())
	if err != nil {
		t.Fatalf("RequeueStaleDeliveries: %v", err)
	}
	if n == 0 {
		t.Fatal("expected at least 1 requeued delivery")
	}

	// Now it should be deliverable again.
	third, err := store.ReceiveDeliveries(ctx, "requeue", "workers", 1, 30_000)
	if err != nil || len(third) == 0 {
		t.Fatalf("post-requeue receive: err=%v count=%d", err, len(third))
	}
	if third[0].MessageID != first[0].MessageID {
		t.Errorf("redelivered different message: got %q want %q", third[0].MessageID, first[0].MessageID)
	}
	if third[0].Attempts < 2 {
		t.Errorf("attempt count must be ≥2, got %d", third[0].Attempts)
	}
}

// TestMongoBulkSendDuplicateIDsAreIdempotent: BulkSendMessages with a
// duplicate ID must not return an error (idempotent upsert via ignoreDuplicates).
func TestMongoBulkSendDuplicateIDsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	msgs := []storage.MessageEntity{
		{ID: "dup-1", Topic: "t", Key: "k", Payload: "first", CreatedAt: time.Now()},
		{ID: "dup-1", Topic: "t", Key: "k", Payload: "second", CreatedAt: time.Now()}, // duplicate
		{ID: "dup-2", Topic: "t", Key: "k", Payload: "unique", CreatedAt: time.Now()},
	}

	if err := store.BulkSendMessages(ctx, msgs); err != nil {
		t.Fatalf("BulkSendMessages with duplicate must not error: %v", err)
	}
}

// TestMongoPingReflectsConnectivity: Ping must succeed when connected and
// is used by the health endpoint to detect storage outages.
func TestMongoPingReflectsConnectivity(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestMongoSubscriptionFanout: a message must create one delivery per
// subscribed consumer group, and each group receives it independently.
func TestMongoSubscriptionFanout(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	groups := []string{"billing", "analytics", "notifications"}
	for _, g := range groups {
		if err := store.UpsertSubscription(ctx, "fanout", g); err != nil {
			t.Fatalf("UpsertSubscription %s: %v", g, err)
		}
	}
	if err := store.SendMessage(ctx, "fanout-msg", "fanout", "k", "broadcast"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := store.CreateDeliveries(ctx, "fanout-msg", "fanout", groups, 3); err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}

	for _, g := range groups {
		deliveries, err := store.ReceiveDeliveries(ctx, "fanout", g, 10, 30_000)
		if err != nil {
			t.Fatalf("ReceiveDeliveries(%s): %v", g, err)
		}
		if len(deliveries) != 1 {
			t.Errorf("group %s: expected 1 delivery, got %d", g, len(deliveries))
		}
	}
}

// TestMongoConcurrentAckRace: two goroutines trying to ack the same receipt
// must not both succeed — exactly one must get ok=true.
func TestMongoConcurrentAckRace(t *testing.T) {
	ctx := context.Background()
	store := startStore(t)

	if err := store.UpsertSubscription(ctx, "race", "g"); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := store.SendMessage(ctx, "race-msg", "race", "k", "x"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := store.CreateDeliveries(ctx, "race-msg", "race", []string{"g"}, 3); err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}

	deliveries, err := store.ReceiveDeliveries(ctx, "race", "g", 1, 30_000)
	if err != nil || len(deliveries) == 0 {
		t.Fatalf("ReceiveDeliveries: err=%v count=%d", err, len(deliveries))
	}
	receiptID := deliveries[0].ReceiptID

	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.AckDelivery(ctx, receiptID)
			if err != nil {
				t.Errorf("AckDelivery: %v", err)
				return
			}
			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("exactly 1 goroutine must win the ack race, got %d", wins)
	}
}
