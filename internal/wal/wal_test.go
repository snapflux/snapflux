package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"vinr.eu/snapflux/internal/config"
)

func walCfg(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		WALPath:         t.TempDir(),
		WALMaxBytes:     4096,
		WALHighWaterPct: 80,
		WALLowWaterPct:  40,
	}
}

func mustNew(t *testing.T, cfg *config.Config) (*WAL, []Entry) {
	t.Helper()
	w, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("wal.New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, pending
}

// TestWALFreshStart: brand-new WAL in empty dir must be empty and accepting.
func TestWALFreshStart(t *testing.T) {
	cfg := walCfg(t)
	w, pending := mustNew(t, cfg)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending entries, got %d", len(pending))
	}
	if !w.IsAccepting() {
		t.Fatal("fresh WAL should be accepting writes")
	}
}

// TestWALCrashRecovery: entries written but never acknowledged must be
// returned as pending on the next open (simulates pod restart).
func TestWALCrashRecovery(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	for i := range 5 {
		_, err := w.Append(Entry{ID: fmt.Sprintf("msg-%d", i), Topic: "t", Key: "k", Payload: "body"})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	_ = w.Close()

	// Reopen without calling Acknowledge — all 5 must be replayed.
	w2, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	if len(pending) != 5 {
		t.Fatalf("expected 5 pending entries after crash, got %d", len(pending))
	}
	for i, e := range pending {
		want := fmt.Sprintf("msg-%d", i)
		if e.ID != want {
			t.Errorf("pending[%d].ID = %q, want %q", i, e.ID, want)
		}
	}
}

// TestWALCheckpointAdvancesReplay: acknowledging through seq N means only
// entries after N are replayed on the next open.
func TestWALCheckpointAdvancesReplay(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	var lastAcked int
	for i := range 6 {
		e, err := w.Append(Entry{ID: fmt.Sprintf("m%d", i), Topic: "t", Key: "k", Payload: i})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if i < 3 {
			lastAcked = e.Seq
		}
	}
	if err := w.Acknowledge(lastAcked); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	_ = w.Close()

	w2, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	if len(pending) != 3 {
		t.Fatalf("expected 3 pending (entries 3–5), got %d", len(pending))
	}
	for _, e := range pending {
		if e.Seq <= lastAcked {
			t.Errorf("replayed entry %q has seq %d ≤ checkpoint %d", e.ID, e.Seq, lastAcked)
		}
	}
}

// TestWALOOMKillCorruption: a partial/corrupt JSON line appended to the WAL
// (e.g. process killed mid-write) must be skipped; valid entries must be recovered.
func TestWALOOMKillCorruption(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	for i := range 4 {
		if _, err := w.Append(Entry{ID: fmt.Sprintf("ok-%d", i), Topic: "t", Key: "k", Payload: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	_ = w.Close()

	// Simulate OOMKill mid-write: append a truncated JSON line.
	f, err := os.OpenFile(filepath.Join(cfg.WALPath, "wal.log"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}
	_, _ = f.WriteString(`{"seq":99,"id":"corrupt","topic":"t"` + "\n") // missing closing brace
	_ = f.Close()

	w2, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	if len(pending) != 4 {
		t.Fatalf("expected 4 valid entries after skipping corrupt line, got %d", len(pending))
	}
	for _, e := range pending {
		if e.ID == "corrupt" {
			t.Error("corrupt entry must not be replayed")
		}
	}
}

// TestWALCorruptCheckpointFile: a corrupt checkpoint file (e.g. partial write)
// must be treated as checkpoint=0 so nothing is skipped.
func TestWALCorruptCheckpointFile(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)
	for i := range 3 {
		if _, err := w.Append(Entry{ID: fmt.Sprintf("e%d", i), Topic: "t", Key: "k", Payload: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	_ = w.Close()

	// Corrupt the checkpoint file.
	if err := os.WriteFile(filepath.Join(cfg.WALPath, "checkpoint"), []byte("not-a-number"), 0644); err != nil {
		t.Fatalf("corrupt checkpoint: %v", err)
	}

	w2, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	if len(pending) != 3 {
		t.Fatalf("corrupt checkpoint must replay all entries, got %d", len(pending))
	}
}

// TestWALBackpressureBlocksAtHighWater: once the WAL reaches the high-water
// mark, Append must return ErrBackpressure.
func TestWALBackpressureBlocksAtHighWater(t *testing.T) {
	cfg := walCfg(t) // maxBytes=4096, highWater=80% ≈ 3276 bytes
	w, _ := mustNew(t, cfg)

	var hitBackpressure bool
	for i := range 100 {
		_, err := w.Append(Entry{ID: fmt.Sprintf("id-%03d", i), Topic: "orders", Key: "k", Payload: "some-payload-data-here"})
		if err == ErrBackpressure {
			hitBackpressure = true
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if !hitBackpressure {
		t.Fatal("expected ErrBackpressure before filling 100 entries in a 4 KiB WAL")
	}
	if w.IsAccepting() {
		t.Error("WAL must not be accepting after hitting high-water mark")
	}
}

// TestWALBackpressureReleasesAfterCompact: acknowledging entries past the
// low-water mark must trigger compact and re-enable writes.
func TestWALBackpressureReleasesAfterCompact(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	// Fill past high-water.
	var lastSeq int
	for {
		e, err := w.Append(Entry{ID: "x", Topic: "t", Key: "k", Payload: "padding-payload"})
		if err == ErrBackpressure {
			break
		}
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastSeq = e.Seq
	}

	if w.IsAccepting() {
		t.Fatal("must be in backpressure before test makes sense")
	}

	// Acknowledge all → triggers compact → currentBytes drops below low-water.
	if err := w.Acknowledge(lastSeq); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	if !w.IsAccepting() {
		t.Error("WAL must resume accepting after compact drains past low-water mark")
	}
}

// TestWALStaleTmpFileHandled: a stale .tmp file left by a previous crash
// during compact must be safely overwritten on the next compact run.
func TestWALStaleTmpFileHandled(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	var lastSeq int
	for {
		e, err := w.Append(Entry{ID: "y", Topic: "t", Key: "k", Payload: "filler"})
		if err == ErrBackpressure {
			break
		}
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastSeq = e.Seq
	}

	// Simulate a stale tmp file left from a previous failed compact.
	tmpPath := filepath.Join(cfg.WALPath, "wal.log.tmp")
	if err := os.WriteFile(tmpPath, []byte("stale garbage\n"), 0644); err != nil {
		t.Fatalf("write stale tmp: %v", err)
	}

	// Acknowledge should trigger compact, which must overwrite the stale tmp.
	if err := w.Acknowledge(lastSeq); err != nil {
		t.Fatalf("Acknowledge (triggering compact): %v", err)
	}

	// Tmp file must be gone after rename.
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("stale .tmp file should have been cleaned up by atomic rename")
	}
}

// TestWALAcknowledgeIdempotent: calling Acknowledge twice with the same seq
// must not corrupt the checkpoint or cause an error.
func TestWALAcknowledgeIdempotent(t *testing.T) {
	cfg := walCfg(t)
	w, _ := mustNew(t, cfg)

	e, err := w.Append(Entry{ID: "id", Topic: "t", Key: "k", Payload: nil})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Acknowledge(e.Seq); err != nil {
		t.Fatalf("first Acknowledge: %v", err)
	}
	if err := w.Acknowledge(e.Seq); err != nil {
		t.Fatalf("second Acknowledge (idempotent): %v", err)
	}
}

// TestWALConcurrentAppends: N goroutines appending concurrently must not race
// or lose entries. Run with -race.
func TestWALConcurrentAppends(t *testing.T) {
	cfg := &config.Config{
		WALPath:         t.TempDir(),
		WALMaxBytes:     1 << 20, // 1 MiB — large enough for the whole test
		WALHighWaterPct: 90,
		WALLowWaterPct:  50,
	}
	w, _ := mustNew(t, cfg)

	const goroutines = 10
	const perGoroutine = 20
	var wg sync.WaitGroup
	errors := make([]error, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				_, err := w.Append(Entry{
					ID:      fmt.Sprintf("g%d-i%d", g, i),
					Topic:   "t",
					Key:     "k",
					Payload: g*perGoroutine + i,
				})
				if err != nil {
					errors[g] = err
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for g, err := range errors {
		if err != nil {
			t.Errorf("goroutine %d: %v", g, err)
		}
	}

	_ = w.Close()

	// Reopen and verify all entries survive.
	w2, pending, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	if len(pending) != goroutines*perGoroutine {
		t.Fatalf("expected %d pending entries, got %d", goroutines*perGoroutine, len(pending))
	}
}

// TestWALCompactReducesSize: after acknowledging all entries the compact run
// must leave the WAL smaller than it was.
func TestWALCompactReducesSize(t *testing.T) {
	cfg := &config.Config{
		WALPath:         t.TempDir(),
		WALMaxBytes:     4096, // small so low-water is quickly exceeded
		WALHighWaterPct: 95,   // high so backpressure doesn't block appends
		WALLowWaterPct:  5,    // low-water = 204 bytes — well below the ~3 KiB written
	}
	w, _ := mustNew(t, cfg)

	var lastSeq int
	for i := range 30 {
		e, err := w.Append(Entry{ID: fmt.Sprintf("msg-%02d", i), Topic: "t", Key: "k", Payload: "body"})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastSeq = e.Seq
	}

	w.mu.Lock()
	beforeBytes := w.currentBytes
	w.mu.Unlock()

	if err := w.Acknowledge(lastSeq); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	w.mu.Lock()
	afterBytes := w.currentBytes
	w.mu.Unlock()

	if afterBytes >= beforeBytes {
		t.Errorf("compact must shrink the WAL: before=%d after=%d", beforeBytes, afterBytes)
	}
}
