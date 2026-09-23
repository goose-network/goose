// Package metrics persists per-request metrics to a local bbolt database and
// maintains a live, in-memory performance view per outbound that selectors
// and filters read on the hot path. The DB write path is asynchronous
// (buffered channel → batched writes) so the data path never blocks on disk.
package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/core"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketRequests  = []byte("requests")
	bucketOutbounds = []byte("outbounds")
)

// Store is the metrics store: a bbolt DB for durable per-request records
// plus an in-memory stats view per outbound for live selection.
type Store struct {
	db *bolt.DB

	// in-mem stats per outbound ID
	mu    sync.RWMutex
	stats map[string]*statsWindow

	// async write pipeline
	in     chan core.RequestMetric
	done   chan struct{}
	sendMu sync.RWMutex // guards s.in against close-during-send
	closed atomic.Bool
}

// statsWindow tracks a rolling window of latency samples for one outbound.
type statsWindow struct {
	mu          sync.Mutex
	samples     []core.LatencySample
	recent      time.Duration // smoothed latency
	success     int           // successes in window
	total       int           // total in window
	lastSuccess time.Time
}

const (
	windowSize = 32
	writeBuf   = 1024
)

// Open opens (or creates) the bbolt DB at path and starts the writer.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("metrics: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketRequests, bucketOutbounds} {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{
		db:    db,
		stats: map[string]*statsWindow{},
		in:    make(chan core.RequestMetric, writeBuf),
		done:  make(chan struct{}),
	}
	go s.loop()
	return s, nil
}

// Close flushes pending writes and closes the DB. After Close returns, any
// in-flight Record calls (e.g. from a connection still bridging when the
// engine shuts down) drop their records instead of panicking on a closed
// channel.
func (s *Store) Close() error {
	// Idempotent: a second Close (e.g. engine.Close called explicitly and
	// again via defer) must not panic on close(s.in) or block on <-s.done
	// after the loop has already exited. The closed flag gates both.
	s.sendMu.Lock()
	if s.closed.Load() {
		s.sendMu.Unlock()
		return nil
	}
	s.closed.Store(true)
	close(s.in)
	s.sendMu.Unlock()
	<-s.done
	return s.db.Close()
}

// Record enqueues a request metric for durable storage and updates the
// in-memory stats view. It never blocks for long (buffered channel) and is
// safe to call concurrently with Close: the sendMu read lock and the closed
// flag together guarantee we never send on a closed channel.
func (s *Store) Record(m core.RequestMetric) error {
	s.observe(m)
	if s.closed.Load() {
		return nil
	}
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed.Load() {
		return nil
	}
	select {
	case s.in <- m:
		return nil
	default:
		return errors.New("metrics: write buffer full, dropping record")
	}
}

// observe updates the in-memory stats window for the first outbound in the
// chain (the hop whose performance this request reflects).
func (s *Store) observe(m core.RequestMetric) {
	if len(m.Chain) == 0 {
		return
	}
	id := m.Chain[0]
	s.mu.RLock()
	w, ok := s.stats[id]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		if w, ok = s.stats[id]; !ok {
			w = &statsWindow{}
			s.stats[id] = w
		}
		s.mu.Unlock()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) >= windowSize {
		w.samples = w.samples[1:]
	}
	w.samples = append(w.samples, core.LatencySample{At: m.StartedAt, Latency: m.Latency, OK: m.Success})
	w.total++
	if m.Success {
		w.success++
		w.lastSuccess = m.FinishedAt
	}
	// exponential smoothing
	if w.recent == 0 {
		w.recent = m.Latency
	} else {
		w.recent = w.recent/2 + m.Latency/2
	}
}

// Stats returns a snapshot of the live performance view for an outbound.
func (s *Store) Stats(id string) core.OutboundStats {
	s.mu.RLock()
	w, ok := s.stats[id]
	s.mu.RUnlock()
	if !ok {
		return core.OutboundStats{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	st := core.OutboundStats{
		RecentLatency: w.recent,
		LastSeen:      w.lastSuccess,
	}
	if w.total > 0 {
		st.SuccessRate = float64(w.success) / float64(w.total)
	}
	st.Samples = append(st.Samples, w.samples...)
	return st
}

// loop drains the write channel and batch-inserts into bbolt.
func (s *Store) loop() {
	defer close(s.done)
	batch := make([]core.RequestMetric, 0, 64)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		_ = s.db.Batch(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketRequests)
			for _, m := range batch {
				key := []byte(fmt.Sprintf("%d-%s", m.StartedAt.UnixNano(), m.ID))
				val, err := json.Marshal(m)
				if err != nil {
					continue
				}
				_ = b.Put(key, val)
			}
			return nil
		})
		batch = batch[:0]
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case m, ok := <-s.in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, m)
			if len(batch) >= 64 {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

// Recent returns the most recent N request metrics from the DB (newest first).
func (s *Store) Recent(n int) ([]core.RequestMetric, error) {
	out := make([]core.RequestMetric, 0, n)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketRequests).Cursor()
		// keys are time-ordered ascending; iterate backwards for newest.
		var keys [][]byte
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			keys = append(keys, k)
			if len(keys) >= n {
				break
			}
		}
		for _, k := range keys {
			v := tx.Bucket(bucketRequests).Get(k)
			var m core.RequestMetric
			if err := json.Unmarshal(v, &m); err == nil {
				out = append(out, m)
			}
		}
		return nil
	})
	return out, err
}
