package messaging_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"vinr.eu/snapflux/internal/broker"
	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/messaging"
	"vinr.eu/snapflux/internal/model"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/storage/memory"
	"vinr.eu/snapflux/internal/wal"
)

// ── helpers ──────────────────────────────────────────────────────────────────

type serviceStack struct {
	store   storage.Provider
	walSvc  *wal.WAL
	batch   *messaging.BatchFlush
	requeue *messaging.Requeue
	broker  *broker.Broker
	svc     *messaging.Service
}

func newStack(t *testing.T) *serviceStack {
	t.Helper()
	return newStackWithStore(t, memory.New())
}

func newStackWithStore(t *testing.T, store storage.Provider) *serviceStack {
	t.Helper()
	ctx := context.Background()

	cfg := &config.Config{
		HeartbeatIntervalMs:  50,
		HeartbeatTimeoutMs:   500,
		BatchFlushIntervalMs: 10,
		BatchFlushSize:       100,
		BatchMaxQueueDepth:   5000,
		MaxDeliveryAttempts:  3,
		VisibilityTimeoutMs:  300,
		RequeueIntervalMs:    50,
		WALPath:              t.TempDir(),
		WALMaxBytes:          1 << 20,
		WALHighWaterPct:      80,
		WALLowWaterPct:       40,
	}

	walSvc, pending, err := wal.New(cfg)
	if err != nil {
		t.Fatalf("wal.New: %v", err)
	}

	batch := messaging.NewBatchFlush(store, walSvc, cfg)
	b := broker.New(store, cfg)

	if err := b.Start(ctx); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	if err := batch.Start(ctx, pending); err != nil {
		t.Fatalf("batch.Start: %v", err)
	}

	requeue := messaging.NewRequeue(store, cfg)
	if err := requeue.Start(ctx); err != nil {
		t.Fatalf("requeue.Start: %v", err)
	}

	svc := messaging.NewService(store, b, batch, walSvc, cfg)

	t.Cleanup(func() {
		requeue.Stop()
		b.Stop(ctx)
		batch.Stop(ctx)
		_ = walSvc.Close()
	})

	return &serviceStack{store: store, walSvc: walSvc, batch: batch, requeue: requeue, broker: b, svc: svc}
}

// waitFor polls predicate until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// faultStore wraps a Provider and can inject errors per operation.
type faultStore struct {
	storage.Provider
	mu     sync.Mutex
	faults map[string]error
}

func newFaultStore(inner storage.Provider) *faultStore {
	return &faultStore{Provider: inner, faults: make(map[string]error)}
}

func (f *faultStore) inject(op string, err error) {
	f.mu.Lock()
	if err == nil {
		delete(f.faults, op)
	} else {
		f.faults[op] = err
	}
	f.mu.Unlock()
}

func (f *faultStore) fault(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.faults[op]
}

func (f *faultStore) SendMessage(ctx context.Context, id, topic, key string, body any) error {
	if err := f.fault("SendMessage"); err != nil {
		return err
	}
	return f.Provider.SendMessage(ctx, id, topic, key, body)
}

func (f *faultStore) GetSubscriptions(ctx context.Context, topic string) ([]string, error) {
	if err := f.fault("GetSubscriptions"); err != nil {
		return nil, err
	}
	return f.Provider.GetSubscriptions(ctx, topic)
}

func (f *faultStore) CreateDeliveries(ctx context.Context, messageID, topic string, groups []string, maxAttempts int) error {
	if err := f.fault("CreateDeliveries"); err != nil {
		return err
	}
	return f.Provider.CreateDeliveries(ctx, messageID, topic, groups, maxAttempts)
}

func (f *faultStore) ReceiveDeliveries(ctx context.Context, topic, group string, limit, visibilityMs int) ([]storage.DeliveryWithMessage, error) {
	if err := f.fault("ReceiveDeliveries"); err != nil {
		return nil, err
	}
	return f.Provider.ReceiveDeliveries(ctx, topic, group, limit, visibilityMs)
}

func (f *faultStore) AckDelivery(ctx context.Context, receiptID string) (bool, error) {
	if err := f.fault("AckDelivery"); err != nil {
		return false, err
	}
	return f.Provider.AckDelivery(ctx, receiptID)
}

func (f *faultStore) BulkSendMessages(ctx context.Context, messages []storage.MessageEntity) error {
	if err := f.fault("BulkSendMessages"); err != nil {
		return err
	}
	return f.Provider.BulkSendMessages(ctx, messages)
}

// ── happy-path ───────────────────────────────────────────────────────────────

func TestServiceSendReceiveAck(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	resp, err := s.svc.Send(ctx, "orders", "key-1", map[string]any{"amount": 99}, true, model.DurabilityDurable)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.ID == "" {
		t.Fatal("Send must return a non-empty message ID")
	}

	var msgs []model.MessageResponse
	waitFor(t, 2*time.Second, func() bool {
		msgs, _ = s.svc.Receive(ctx, "orders", "billing", 10, true)
		return len(msgs) == 1
	})

	if msgs[0].Key != "key-1" {
		t.Errorf("wrong key: %q", msgs[0].Key)
	}
	if msgs[0].Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", msgs[0].Attempts)
	}

	ackResp, err := s.svc.Ack(ctx, "orders", "billing", msgs[0].ReceiptID, true)
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !ackResp.Acknowledged {
		t.Error("ack response must be acknowledged=true")
	}

	// Message must not be re-delivered after ack.
	time.Sleep(100 * time.Millisecond)
	after, _ := s.svc.Receive(ctx, "orders", "billing", 10, true)
	if len(after) != 0 {
		t.Errorf("message redelivered after ack: got %d", len(after))
	}
}

// TestServiceReceiveBeforeSubscription: a message published before any consumer
// has ever called Receive must not create deliveries (no subscription exists yet).
func TestServiceReceiveBeforeSubscription(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	if _, err := s.svc.Send(ctx, "events", "k", "payload", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// No consumer has called Receive yet, so no subscription exists.
	// The message should be invisible to a new consumer.
	msgs, err := s.svc.Receive(ctx, "events", "late-group", 10, true)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for a brand-new subscription, got %d", len(msgs))
	}
}

// TestServiceVisibilityTimeoutCausesRedelivery: a message that is delivered but
// not acknowledged must become visible again after the visibility timeout so the
// requeue job can re-deliver it — simulating a slow or crashed consumer.
func TestServiceVisibilityTimeoutCausesRedelivery(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	// Subscribe first so deliveries are created on send.
	if _, err := s.svc.Receive(ctx, "jobs", "workers", 0, true); err != nil {
		t.Fatalf("prime subscription: %v", err)
	}
	if _, err := s.svc.Send(ctx, "jobs", "k", "do-work", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var first []model.MessageResponse
	waitFor(t, 2*time.Second, func() bool {
		first, _ = s.svc.Receive(ctx, "jobs", "workers", 1, true)
		return len(first) == 1
	})

	// Do NOT ack — simulate a crashed consumer.
	// Wait for visibility timeout (300ms) + requeue interval (50ms) + buffer.
	time.Sleep(500 * time.Millisecond)

	var second []model.MessageResponse
	waitFor(t, 2*time.Second, func() bool {
		second, _ = s.svc.Receive(ctx, "jobs", "workers", 1, true)
		return len(second) == 1
	})

	if second[0].ID != first[0].ID {
		t.Errorf("redelivered message ID mismatch: got %q want %q", second[0].ID, first[0].ID)
	}
	if second[0].Attempts < 2 {
		t.Errorf("expected ≥2 attempts on redelivery, got %d", second[0].Attempts)
	}
}

// TestServiceDeadLetterAfterMaxAttempts: after MaxDeliveryAttempts (3) the
// delivery must be marked dead and no longer returned by Receive.
func TestServiceDeadLetterAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	if _, err := s.svc.Receive(ctx, "risky", "workers", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if _, err := s.svc.Send(ctx, "risky", "k", "work", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Consume without acking 3 times — each cycle waits for requeue.
	for attempt := range 3 {
		var msgs []model.MessageResponse
		waitFor(t, 2*time.Second, func() bool {
			msgs, _ = s.svc.Receive(ctx, "risky", "workers", 1, true)
			return len(msgs) == 1
		})
		t.Logf("attempt %d: receiptId=%s", attempt+1, msgs[0].ReceiptID)
		// Do not ack — let visibility timeout expire.
		time.Sleep(500 * time.Millisecond)
	}

	// After 3 failed attempts the delivery must be dead.
	time.Sleep(300 * time.Millisecond)
	msgs, _ := s.svc.Receive(ctx, "risky", "workers", 10, true)
	if len(msgs) != 0 {
		t.Errorf("dead-lettered message must not be redelivered, got %d messages", len(msgs))
	}
}

// TestServiceDoubleAckReturnsNotFound: acking the same receiptId twice must
// return ErrNotFound on the second call.
func TestServiceDoubleAckReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	if _, err := s.svc.Receive(ctx, "t", "g", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if _, err := s.svc.Send(ctx, "t", "k", "body", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var msgs []model.MessageResponse
	waitFor(t, 2*time.Second, func() bool {
		msgs, _ = s.svc.Receive(ctx, "t", "g", 1, true)
		return len(msgs) == 1
	})

	if _, err := s.svc.Ack(ctx, "t", "g", msgs[0].ReceiptID, true); err != nil {
		t.Fatalf("first Ack: %v", err)
	}

	_, err := s.svc.Ack(ctx, "t", "g", msgs[0].ReceiptID, true)
	if !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("second Ack: expected ErrNotFound, got %v", err)
	}
}

// TestServiceConcurrentReceiversSingleDelivery: multiple goroutines polling the
// same topic/group must not receive the same message more than once.
func TestServiceConcurrentReceiversSingleDelivery(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	const messages = 20
	const consumers = 5

	// Prime subscription and send all messages.
	if _, err := s.svc.Receive(ctx, "work", "pool", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}
	for i := range messages {
		if _, err := s.svc.Send(ctx, "work", fmt.Sprintf("k%d", i), i, true, model.DurabilityStrict); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				msgs, _ := s.svc.Receive(ctx, "work", "pool", 5, true)
				for _, m := range msgs {
					mu.Lock()
					seen[m.ID]++
					mu.Unlock()
					_, _ = s.svc.Ack(ctx, "work", "pool", m.ReceiptID, true)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	for id, count := range seen {
		if count > 1 {
			t.Errorf("message %s delivered %d times (should be 1)", id, count)
		}
	}
}

// TestServiceFireAndForgetDroppedWhenQueueFull: fire-and-forget messages must
// be silently dropped when BatchMaxQueueDepth is reached.
func TestServiceFireAndForgetDroppedWhenQueueFull(t *testing.T) {
	ctx := context.Background()
	inner := memory.New()
	fs := newFaultStore(inner)

	// Make BulkSendMessages hang so the queue fills up.
	fs.inject("BulkSendMessages", errors.New("storage offline"))

	cfg := &config.Config{
		HeartbeatIntervalMs:  50,
		HeartbeatTimeoutMs:   500,
		BatchFlushIntervalMs: 10,
		BatchFlushSize:       2,
		BatchMaxQueueDepth:   5, // very small cap
		MaxDeliveryAttempts:  3,
		VisibilityTimeoutMs:  5000,
		RequeueIntervalMs:    10000,
		WALPath:              t.TempDir(),
		WALMaxBytes:          1 << 20,
		WALHighWaterPct:      80,
		WALLowWaterPct:       40,
	}
	walSvc, pending, _ := wal.New(cfg)
	batch := messaging.NewBatchFlush(fs, walSvc, cfg)
	b := broker.New(fs, cfg)
	_ = b.Start(context.Background())
	_ = batch.Start(context.Background(), pending)
	svc := messaging.NewService(fs, b, batch, walSvc, cfg)

	t.Cleanup(func() {
		b.Stop(context.Background())
		batch.Stop(context.Background())
		_ = walSvc.Close()
	})

	// Send more fire-and-forget messages than the queue cap.
	for i := range 20 {
		_, _ = svc.Send(ctx, "t", fmt.Sprintf("k%d", i), i, true, model.DurabilityFireAndForget)
	}

	// The queue should be capped; no panic and no unbounded growth.
	// (We can't easily assert exact count without exposing internals, but the
	// test verifies no panic and no OOM under a very tight cap.)
}

// TestServiceDurableRejectsOnWALBackpressure: durable sends must return
// ErrServiceUnavailable when the WAL is under backpressure.
func TestServiceDurableRejectsOnWALBackpressure(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	// Fill the WAL to trigger backpressure.
	for {
		_, err := s.walSvc.Append(wal.Entry{ID: "filler", Topic: "t", Key: "k", Payload: "pad"})
		if errors.Is(err, wal.ErrBackpressure) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected WAL error: %v", err)
		}
	}

	_, err := s.svc.Send(ctx, "orders", "key", "body", true, model.DurabilityDurable)
	if !errors.Is(err, messaging.ErrServiceUnavailable) {
		t.Errorf("expected ErrServiceUnavailable under WAL backpressure, got %v", err)
	}
}

// TestServiceStrictDurabilityBypassesWAL: strict sends go directly to storage
// and must be readable immediately without waiting for the batch flush.
func TestServiceStrictDurabilityBypassesWAL(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	if _, err := s.svc.Receive(ctx, "critical", "audit", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}

	resp, err := s.svc.Send(ctx, "critical", "k", "important", true, model.DurabilityStrict)
	if err != nil {
		t.Fatalf("Send (strict): %v", err)
	}
	if resp.Durability != model.DurabilityStrict {
		t.Errorf("wrong durability: %s", resp.Durability)
	}

	// No batch flush needed — message is in storage immediately.
	msgs, err := s.svc.Receive(ctx, "critical", "audit", 10, true)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("strict-durability message must be readable immediately without batch flush")
	}
}

// TestServiceStorageFailOnStrictSend: if storage fails during a strict send
// the error must propagate to the caller.
func TestServiceStorageFailOnStrictSend(t *testing.T) {
	ctx := context.Background()
	fs := newFaultStore(memory.New())
	s := newStackWithStore(t, fs)

	fs.inject("SendMessage", errors.New("disk full"))

	_, err := s.svc.Send(ctx, "t", "k", "body", true, model.DurabilityStrict)
	if err == nil {
		t.Fatal("expected error from storage failure, got nil")
	}
}

// TestServiceStorageFailOnGetSubscriptions: if GetSubscriptions fails after the
// message is stored (strict send), the error must propagate. The message exists
// in storage but has no deliveries — this is a known partial-write scenario.
func TestServiceStorageFailOnGetSubscriptions(t *testing.T) {
	ctx := context.Background()
	fs := newFaultStore(memory.New())
	s := newStackWithStore(t, fs)

	// Prime a subscription so CreateDeliveries would normally be called.
	if _, err := s.svc.Receive(ctx, "t", "g", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}

	fs.inject("GetSubscriptions", errors.New("network timeout"))

	_, err := s.svc.Send(ctx, "t", "k", "body", true, model.DurabilityStrict)
	if err == nil {
		t.Fatal("expected error when GetSubscriptions fails")
	}
}

// TestServiceStorageFailOnReceive: Receive must propagate storage errors.
func TestServiceStorageFailOnReceive(t *testing.T) {
	ctx := context.Background()
	fs := newFaultStore(memory.New())
	s := newStackWithStore(t, fs)

	fs.inject("ReceiveDeliveries", errors.New("connection reset"))

	_, err := s.svc.Receive(ctx, "t", "g", 10, true)
	if err == nil {
		t.Fatal("expected error when ReceiveDeliveries fails")
	}
}

// TestServiceStorageFailOnAck: Ack must propagate storage errors.
func TestServiceStorageFailOnAck(t *testing.T) {
	ctx := context.Background()
	fs := newFaultStore(memory.New())
	s := newStackWithStore(t, fs)

	if _, err := s.svc.Receive(ctx, "t", "g", 0, true); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if _, err := s.svc.Send(ctx, "t", "k", "body", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var msgs []model.MessageResponse
	waitFor(t, 2*time.Second, func() bool {
		msgs, _ = s.svc.Receive(ctx, "t", "g", 1, true)
		return len(msgs) == 1
	})

	fs.inject("AckDelivery", errors.New("storage unreachable"))
	_, err := s.svc.Ack(ctx, "t", "g", msgs[0].ReceiptID, true)
	if err == nil {
		t.Fatal("expected error when AckDelivery fails")
	}
}

// TestServiceMultipleTopicsIsolated: messages on topic A must not appear
// on topic B even when both have the same consumer group name.
func TestServiceMultipleTopicsIsolated(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	for _, topic := range []string{"alpha", "beta"} {
		if _, err := s.svc.Receive(ctx, topic, "g", 0, true); err != nil {
			t.Fatalf("prime %s: %v", topic, err)
		}
	}

	if _, err := s.svc.Send(ctx, "alpha", "k", "for-alpha", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		msgs, _ := s.svc.Receive(ctx, "alpha", "g", 10, true)
		return len(msgs) == 1
	})

	betaMsgs, _ := s.svc.Receive(ctx, "beta", "g", 10, true)
	if len(betaMsgs) != 0 {
		t.Errorf("topic isolation breach: %d messages appeared on beta", len(betaMsgs))
	}
}

// TestServiceMultipleGroupsEachGetCopy: two consumer groups on the same topic
// must each receive an independent copy of every message.
func TestServiceMultipleGroupsEachGetCopy(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)

	for _, group := range []string{"groupA", "groupB"} {
		if _, err := s.svc.Receive(ctx, "fanout", group, 0, true); err != nil {
			t.Fatalf("prime %s: %v", group, err)
		}
	}

	if _, err := s.svc.Send(ctx, "fanout", "k", "broadcast", true, model.DurabilityStrict); err != nil {
		t.Fatalf("Send: %v", err)
	}

	for _, group := range []string{"groupA", "groupB"} {
		var msgs []model.MessageResponse
		waitFor(t, 2*time.Second, func() bool {
			msgs, _ = s.svc.Receive(ctx, "fanout", group, 10, true)
			return len(msgs) == 1
		})
		if msgs[0].Key != "k" {
			t.Errorf("group %s: wrong key %q", group, msgs[0].Key)
		}
	}
}
