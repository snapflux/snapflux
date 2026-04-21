package wal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"vinr.eu/snapflux/internal/config"
)

type Entry struct {
	Seq        int    `json:"seq"`
	ID         string `json:"id"`
	Topic      string `json:"topic"`
	Key        string `json:"key"`
	Payload    any    `json:"payload"`
	Durability string `json:"durability"`
	Timestamp  string `json:"timestamp"`
}

var ErrBackpressure = errors.New("WAL high-water mark reached — broker under backpressure")

type WAL struct {
	walFile        string
	checkpointFile string
	maxBytes       int64
	highWaterBytes int64
	lowWaterBytes  int64

	mu            sync.Mutex
	fd            *os.File
	seq           int
	checkpointSeq int
	currentBytes  int64
	accepting     bool

	appends            metric.Int64Counter
	acknowledges       metric.Int64Counter
	backpressureEvents metric.Int64Counter
}

func New(cfg *config.Config) (*WAL, []Entry, error) {
	if err := os.MkdirAll(cfg.WALPath, 0755); err != nil {
		return nil, nil, err
	}

	w := &WAL{
		walFile:        filepath.Join(cfg.WALPath, "wal.log"),
		checkpointFile: filepath.Join(cfg.WALPath, "checkpoint"),
		maxBytes:       cfg.WALMaxBytes,
		highWaterBytes: cfg.WALMaxBytes * int64(cfg.WALHighWaterPct) / 100,
		lowWaterBytes:  cfg.WALMaxBytes * int64(cfg.WALLowWaterPct) / 100,
		accepting:      true,
	}

	cp, err := w.readCheckpoint()
	if err != nil {
		return nil, nil, err
	}
	w.checkpointSeq = cp

	pending, err := w.replay()
	if err != nil {
		return nil, nil, err
	}

	fd, err := os.OpenFile(w.walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	w.fd = fd
	w.accepting = w.currentBytes < w.highWaterBytes

	meter := otel.GetMeterProvider().Meter("snapflux/wal")
	w.appends, _ = meter.Int64Counter("snapflux.wal.appends",
		metric.WithDescription("Total WAL entries appended"))
	w.acknowledges, _ = meter.Int64Counter("snapflux.wal.acknowledges",
		metric.WithDescription("Total WAL checkpoints advanced"))
	w.backpressureEvents, _ = meter.Int64Counter("snapflux.wal.backpressure.events",
		metric.WithDescription("Number of times WAL entered backpressure"))
	_, _ = meter.Int64ObservableGauge("snapflux.wal.bytes",
		metric.WithDescription("Current WAL file size in bytes"),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			w.mu.Lock()
			b := w.currentBytes
			w.mu.Unlock()
			o.Observe(b)
			return nil
		}),
	)

	slog.Info("WAL ready", "pending", len(pending), "bytes", w.currentBytes, "accepting", w.accepting)
	return w, pending, nil
}

func (w *WAL) Append(entry Entry) (Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.accepting {
		return Entry{}, ErrBackpressure
	}

	w.seq++
	entry.Seq = w.seq
	line, err := json.Marshal(entry)
	if err != nil {
		return Entry{}, err
	}
	line = append(line, '\n')

	if _, err := w.fd.Write(line); err != nil {
		return Entry{}, err
	}
	if err := w.fd.Sync(); err != nil {
		return Entry{}, err
	}

	w.currentBytes += int64(len(line))
	w.updateBackpressure()
	w.appends.Add(context.Background(), 1)
	return entry, nil
}

func (w *WAL) Acknowledge(lastSeq int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if lastSeq <= w.checkpointSeq {
		return nil
	}
	w.checkpointSeq = lastSeq
	if err := os.WriteFile(w.checkpointFile, []byte(strconv.Itoa(lastSeq)), 0644); err != nil {
		return err
	}
	w.acknowledges.Add(context.Background(), 1)
	if w.currentBytes >= w.lowWaterBytes {
		return w.compact()
	}
	return nil
}

func (w *WAL) IsAccepting() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accepting
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fd != nil {
		return w.fd.Close()
	}
	return nil
}

func (w *WAL) readCheckpoint() (int, error) {
	data, err := os.ReadFile(w.checkpointFile)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, nil
	}
	return n, nil
}

func (w *WAL) replay() ([]Entry, error) {
	content, err := os.ReadFile(w.walFile)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var pending []Entry
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue // skip corrupted tail bytes from a crash mid-write
		}
		if e.Seq > w.seq {
			w.seq = e.Seq
		}
		if e.Seq > w.checkpointSeq {
			pending = append(pending, e)
		}
	}
	w.currentBytes = int64(len(content))
	return pending, nil
}

func (w *WAL) compact() error {
	content, err := os.ReadFile(w.walFile)
	if err != nil {
		return err
	}

	var kept []string
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e Entry
		if json.Unmarshal([]byte(line), &e) == nil && e.Seq > w.checkpointSeq {
			kept = append(kept, line)
		}
	}

	compacted := ""
	if len(kept) > 0 {
		compacted = strings.Join(kept, "\n") + "\n"
	}

	_ = w.fd.Close()
	if err := os.WriteFile(w.walFile, []byte(compacted), 0644); err != nil {
		return err
	}
	fd, err := os.OpenFile(w.walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.fd = fd
	w.currentBytes = int64(len(compacted))
	w.updateBackpressure()
	slog.Info("WAL compacted", "bytes", w.currentBytes)
	return nil
}

func (w *WAL) updateBackpressure() {
	if !w.accepting && w.currentBytes < w.lowWaterBytes {
		w.accepting = true
		slog.Info("WAL drained below low-water mark — resuming writes")
	} else if w.accepting && w.currentBytes >= w.highWaterBytes {
		w.accepting = false
		w.backpressureEvents.Add(context.Background(), 1)
		slog.Warn("WAL high-water mark reached — entering backpressure", "bytes", w.currentBytes, "limit", w.highWaterBytes)
	}
}
