// Package metrics persists per-request metrics to a local bbolt database and
// maintains a live, in-memory performance view per outbound that selectors
// and filters read on the hot path. The DB write path is asynchronous
// (buffered channel → batched writes) so the data path never blocks on disk.
package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/core"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketRequests      = []byte("requests")
	bucketOutbounds     = []byte("outbounds")
	bucketExitChecks    = []byte("exitchecks")
	bucketProbes        = []byte("probes")
	bucketFullProbes    = []byte("fullprobes")
	bucketFullProbeRows = []byte("fullproberows")
)

// ErrClosed is returned by the synchronous record paths after Close.
// Callers that retry a failed write (the full-probe runner's terminal
// persist) match it with errors.Is to stop retrying a store that will never
// accept the record again.
var ErrClosed = errors.New("metrics: store closed")

// Store is the metrics store: a bbolt DB for durable per-request records
// plus an in-memory stats view per outbound for live selection.
type Store struct {
	db *bolt.DB

	// in-mem stats per outbound ID
	mu    sync.RWMutex
	stats map[string]*statsWindow
	// reach is the latest measured local-reachability verdict per outbound
	// ID, derived from the recorded metric rows (see observe). Routing
	// filters a pool's first-hop candidates by it.
	reach map[string]reachVerdict

	// async write pipeline
	in     chan core.RequestMetric
	done   chan struct{}
	sendMu sync.RWMutex // guards s.in against close-during-send
	closed atomic.Bool

	// probeIn / probeDone are the probe-row pipeline: HTTP probe records and
	// full-probe job snapshots share one batched writer, so a full active
	// probing sweep issuing thousands of requests per second never pays a
	// synchronous fsync per row (the request pipeline's lesson, applied to
	// the probe bucket). probeSendMu / probeClosed guard it against
	// close-during-send exactly like sendMu / closed.
	probeIn     chan probeWrite
	probeDone   chan struct{}
	probeSendMu sync.RWMutex
	probeClosed atomic.Bool

	// reqRows / probeRows track the durable row counts for retention
	// pruning; Open seeds them from the buckets before the writers start.
	// reqRows is owned by loop(), bucketRequests' sole writer. bucketProbes
	// has TWO writers — probeLoop's async flush and the synchronous
	// RecordHTTPProbe — so probeRows is atomic: each write transaction
	// computes its delta (new rows minus prunes) from the current count and
	// publishes it only after commit. probeWriteMu spans each write
	// transaction AND its counter publish, so a writer's baseline always
	// equals the committed row count: bbolt serializes the transactions and
	// the mutex closes the commit-to-Add gap, making prune decisions exact
	// (a stale baseline could otherwise over- or under-prune by the other
	// writer's in-flight delta). A Put that overwrites an existing key adds
	// no row (probe IDs are timestamp+route+target and can still collide),
	// so both writers Get before counting.
	reqRows      int64
	probeRows    atomic.Int64
	probeWriteMu sync.Mutex

	// fpJobRows / fpRowsTotal track the full-probe attempt rows stored under
	// bucketFullProbeRows (one nested bucket per job ID). probeLoop owns them;
	// probeWriteMu spans each write transaction AND the counter publish, and
	// every db.Batch attempt reloads the baselines, exactly like probeRows —
	// so prune and eviction decisions are exact. The sync RecordHTTPProbe
	// path never writes job rows (full-probe attempts are always async), so
	// it does not touch these counters.
	fpJobRows   map[string]int64
	fpRowsTotal atomic.Int64
}

// probeWrite is one row for the probe pipeline: exactly one of probe / job
// is set. Both land in time-ordered buckets the readers scan newest-first.
type probeWrite struct {
	probe *core.HTTPProbe
	job   *core.FullProbeJob
}

// reachVerdict is one outbound's latest local-reachability measurement.
type reachVerdict struct {
	ok bool
	at time.Time
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
	// probeWriteBuf buffers probe rows between the sweep and the batched
	// writer. A full-probe job at the 10000 rps cap with a 500 ms flush
	// window needs ~5000 slots of headroom; twice that absorbs a slow disk
	// burst before rows start dropping.
	probeWriteBuf = 8192
	// probeBatchSize flushes the probe pipeline once this many rows queue,
	// mirroring the request pipeline's 64-row batch.
	probeBatchSize = 64
	// fullProbeHistory caps how many finished full-probe jobs the bucket
	// keeps; older ones are pruned when a terminal snapshot lands. Running
	// jobs and jobs whose row buckets still exist are exempt (see
	// pruneFullProbeJobs), so the cap is soft by that bounded count.
	fullProbeHistory = 64
	// requestHistory / probeHistory cap the durable row buckets. A full
	// probe sweep at the 10000 rps limit writes thousands of success rows
	// per second; without a cap the DB grows without bound. The caps are
	// row counts (a few tens of MB), pruned oldest-first inside the flush
	// that crosses them — a cursor walk, never a per-row scan.
	requestHistory = 20000
	probeHistory   = 20000
	// fullProbeRowsPerJob caps one job's durable attempt rows. It equals
	// FullProbeMaxTotalAttempts, so a job at its attempt cap loses nothing
	// in-job; the prune is belt-and-suspenders against ID collisions across
	// jobs writing the same bucket.
	fullProbeRowsPerJob = 500000
	// fullProbeRowJobs caps how many jobs' result buckets are kept, and
	// fullProbeRowsTotal caps the rows across all of them (the global disk
	// budget). Crossing either evicts the oldest FINISHED job buckets
	// wholesale — a running job's rows are never evicted (at most
	// FullProbeMaxRunningJobs run at once, so the transient worst case is
	// bounded: two maxed running jobs ≈ 1M rows).
	fullProbeRowJobs   = 8
	fullProbeRowsTotal = 500000
)

// Open opens (or creates) the bbolt DB at path and starts the writer.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("metrics: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketRequests, bucketOutbounds, bucketExitChecks, bucketProbes, bucketFullProbes, bucketFullProbeRows} {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		// A job persisted as "running" belongs to a runner in a process
		// that is gone — this Open just claimed the DB's exclusive lock.
		// Reconcile it to a terminal state, or it would read as running
		// forever: apply refuses it (409), cancel finds no runner (409),
		// and the UI polls it endlessly.
		return reconcileFullProbeZombies(tx.Bucket(bucketFullProbes))
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{
		db:        db,
		stats:     map[string]*statsWindow{},
		reach:     map[string]reachVerdict{},
		in:        make(chan core.RequestMetric, writeBuf),
		done:      make(chan struct{}),
		probeIn:   make(chan probeWrite, probeWriteBuf),
		probeDone: make(chan struct{}),
		fpJobRows: map[string]int64{},
	}
	// Seed the retention counters from the existing buckets (keys only —
	// no row decoding). A pre-cap DB may be over the limit; the first flush
	// prunes it back.
	_ = db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketRequests).ForEach(func(k, _ []byte) error { s.reqRows++; return nil })
		var n int64
		_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { n++; return nil })
		s.probeRows.Store(n)
		var total int64
		_ = tx.Bucket(bucketFullProbeRows).ForEachBucket(func(k []byte) error {
			var rows int64
			_ = tx.Bucket(bucketFullProbeRows).Bucket(k).ForEach(func(_, _ []byte) error { rows++; return nil })
			s.fpJobRows[string(k)] = rows
			total += rows
			return nil
		})
		s.fpRowsTotal.Store(total)
		return nil
	})
	// A DB left over the job/row budget (an older build, or a crash between
	// flushes) is pruned once here, before the writers start. Open already
	// reconciled every running snapshot to a terminal state and no runner
	// exists in this process yet, so a bucket whose snapshot is missing
	// belongs to a crashed job, not a live one — safe to evict (live=false).
	if len(s.fpJobRows) > fullProbeRowJobs || s.fpRowsTotal.Load() > fullProbeRowsTotal {
		// A failed commit keeps the seeded (over-budget) counters; the first
		// flush prunes again. Publishing counters reduced by a rolled-back
		// transaction would undercount the real rows forever.
		_ = s.pruneFullProbeRowsAtOpen()
	}
	go s.loop()
	go s.probeLoop()
	return s, nil
}

// pruneFullProbeRowsAtOpen is Open's one-shot retention pass, factored out so
// the commit-failure path is unit-testable. It mirrors the flush discipline:
// eviction runs on a COPY of the seeded baselines inside the transaction, and
// the live map/total are replaced only after Update commits. bbolt rolls back
// on commit failure (e.g. ENOSPC), so mutating s.fpJobRows / fpRowsTotal
// inside the tx would leave the counters permanently below the committed row
// counts — and the still-present buckets invisible to flush eviction (which
// trusts the counters) until the next restart. No lock guards the publish:
// Open starts loop()/probeLoop() only after this returns, so no writer can
// observe the counters yet.
func (s *Store) pruneFullProbeRowsAtOpen() error {
	var jobRows map[string]int64
	var total int64
	err := s.db.Update(func(tx *bolt.Tx) error {
		var e error
		jobRows, total, e = s.pruneFullProbeRowsTx(tx)
		return e
	})
	if err != nil {
		return err
	}
	s.fpJobRows = jobRows
	s.fpRowsTotal.Store(total)
	return nil
}

// pruneFullProbeRowsTx is the prune's transaction body, split from the
// publish so a test can run the eviction and then force a rollback — a
// closed DB fails in Begin, before the body ever runs, and cannot catch an
// in-tx mutation. It must only touch the copy and the tx buckets: the live
// s.fpJobRows / fpRowsTotal are published by the caller after commit, never
// here, or a rolled-back eviction would undercount the surviving rows.
func (s *Store) pruneFullProbeRowsTx(tx *bolt.Tx) (map[string]int64, int64, error) {
	jobRows := make(map[string]int64, len(s.fpJobRows))
	for id, n := range s.fpJobRows {
		jobRows[id] = n
	}
	total := evictFullProbeRowBuckets(tx.Bucket(bucketFullProbeRows), tx.Bucket(bucketFullProbes), jobRows, s.fpRowsTotal.Load(), false)
	return jobRows, total, nil
}

// reconcileFullProbeZombies lands every persisted running-state job in
// state "error" with an explanatory message. Called once at Open, inside
// the bucket-setup transaction: any runner that owned these jobs died with
// the previous process, so their in-flight sweep is over whether or not a
// terminal snapshot made it to disk.
func reconcileFullProbeZombies(fb *bolt.Bucket) error {
	type fix struct {
		key []byte
		val []byte
	}
	var fixes []fix
	err := fb.ForEach(func(k, v []byte) error {
		var j core.FullProbeJob
		if err := json.Unmarshal(v, &j); err != nil {
			return nil
		}
		if j.State != core.FullProbeRunning {
			return nil
		}
		j.State = core.FullProbeError
		j.Error = "engine restarted before the sweep finished"
		if j.FinishedAt.IsZero() {
			j.FinishedAt = time.Now()
		}
		val, err := json.Marshal(j)
		if err != nil {
			return nil
		}
		fixes = append(fixes, fix{key: append([]byte(nil), k...), val: val})
		return nil
	})
	if err != nil {
		return err
	}
	for _, f := range fixes {
		if err := fb.Put(f.key, f.val); err != nil {
			return err
		}
	}
	return nil
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
	// The probe pipeline is closed second and joined separately: a probe
	// row enqueued while the request pipeline was draining still lands.
	s.probeSendMu.Lock()
	if !s.probeClosed.Load() {
		s.probeClosed.Store(true)
		close(s.probeIn)
	}
	s.probeSendMu.Unlock()
	<-s.probeDone
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

// observe updates the in-memory views for the outbound a metric row
// measures. A request row's first hop (Chain[0]) is the outbound whose
// performance and local reachability the row reflects; a probe row
// (Chain[0] == core.HopLocal) measures Chain[1] instead — the engine's own
// network dialing that outbound's server.
//
// Reachability verdicts are recorded from probe rows (both outcomes: a probe
// is a bare TCP dial, so a failure really means unreachable) and from
// successful request rows (a first hop that served traffic was reachable),
// so real traffic revives an outbound a stale failed probe dropped, while a
// handshake-level request failure never marks a server unreachable.
//
// HTTP probe rows (Chain[0] == core.HopProbe) are skipped entirely: they
// measure a target THROUGH a route, so a failure may be the target's, and
// neither the latency (target-bound, not hop-bound) nor the verdict belongs
// to an outbound's stats.
func (s *Store) observe(m core.RequestMetric) {
	if len(m.Chain) == 0 {
		return
	}
	id := m.Chain[0]
	var probe bool
	switch id {
	case core.HopProbe:
		return
	case core.HopLocal:
		if len(m.Chain) < 2 {
			return
		}
		probe = true
		id = m.Chain[1]
	}

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
	w.mu.Unlock()

	if probe || m.Success {
		s.mu.Lock()
		if prev, seen := s.reach[id]; !seen || !m.StartedAt.Before(prev.at) {
			s.reach[id] = reachVerdict{ok: m.Success, at: m.StartedAt}
		}
		s.mu.Unlock()
	}
}

// Reachability returns the latest measured verdict for dialing an outbound's
// proxy server from the engine's local network, derived from the recorded
// metric rows (probe rows and successful first hops). measured is false when
// no row has touched the outbound yet; callers fail open in that case.
func (s *Store) Reachability(id string) (ok bool, at time.Time, measured bool) {
	s.mu.RLock()
	v, seen := s.reach[id]
	s.mu.RUnlock()
	return v.ok, v.at, seen
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
		rows := s.reqRows
		err := s.db.Batch(func(tx *bolt.Tx) error {
			// Batch may re-run fn in a fresh tx if a batch-mate fails;
			// recount from the committed baseline each attempt.
			rows = s.reqRows
			b := tx.Bucket(bucketRequests)
			for _, m := range batch {
				key := []byte(fmt.Sprintf("%d-%s", m.StartedAt.UnixNano(), m.ID))
				val, err := json.Marshal(m)
				if err != nil {
					continue
				}
				if b.Put(key, val) == nil {
					rows++
				}
			}
			rows = pruneTimeBucket(b, rows, requestHistory)
			return nil
		})
		if err == nil {
			s.reqRows = rows
		}
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

// RecordExitCheck durably stores one exit-IP check result. Checks run at
// human cadence (a round per cron tick), so a synchronous single-key write
// is appropriate — unlike request metrics, there is no hot data path to
// protect.
func (s *Store) RecordExitCheck(c core.ExitCheck) error {
	if s.closed.Load() {
		return errors.New("metrics: store closed")
	}
	val, err := json.Marshal(c)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%d-%s", c.StartedAt.UnixNano(), c.ID))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketExitChecks).Put(key, val)
	})
}

// RecentExitChecks returns up to n exit-IP check records, newest first.
// When outboundID is non-empty, only that outbound's records are returned.
func (s *Store) RecentExitChecks(n int, outboundID string) ([]core.ExitCheck, error) {
	if n < 1 {
		n = 1
	}
	out := make([]core.ExitCheck, 0, n)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketExitChecks).Cursor()
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var ec core.ExitCheck
			if err := json.Unmarshal(v, &ec); err == nil {
				if outboundID != "" && ec.OutboundID != outboundID {
					continue
				}
				out = append(out, ec)
				if len(out) >= n {
					return nil
				}
			}
		}
		return nil
	})
	return out, err
}

// RecordHTTPProbe durably stores one HTTP probe result. Like exit-IP checks,
// plain probe rounds run at human cadence, so a synchronous single-key write
// is appropriate — there is no hot data path to protect, and callers (and
// tests) rely on the row being readable the instant the call returns. The
// full active probing sweep, which can issue thousands of rows per second,
// uses RecordHTTPProbeAsync instead.
func (s *Store) RecordHTTPProbe(p core.HTTPProbe) error {
	if s.closed.Load() {
		return errors.New("metrics: store closed")
	}
	val, err := json.Marshal(p)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%d-%s", p.StartedAt.UnixNano(), p.ID))
	// This sync path shares bucketProbes with probeLoop, so it must count
	// and prune too — otherwise periodic probe rounds grow the bucket
	// without bound (no async flush ever crosses the counter cap) and drift
	// the shared counter below the true row count. The mutex spans the
	// transaction and the counter publish so the baseline is exact; see
	// the probeRows field comment.
	var delta int64
	s.probeWriteMu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketProbes)
		base := s.probeRows.Load()
		rows := base
		if b.Get(key) == nil {
			rows++ // a genuinely new row; an overwrite Put adds none
		}
		if err := b.Put(key, val); err != nil {
			return err
		}
		rows = pruneTimeBucket(b, rows, probeHistory)
		delta = rows - base
		return nil
	})
	if err == nil {
		s.probeRows.Add(delta)
	}
	s.probeWriteMu.Unlock()
	return err
}

// RecordHTTPProbeAsync enqueues one HTTP probe result on the batched probe
// pipeline. It is the full-probe sweep's persistence path: at up to 10000
// requests/second a synchronous fsync per row would throttle the sweep to the
// disk's commit rate, so rows are batched like request metrics. Safe to call
// concurrently with Close (probeSendMu + probeClosed); a row offered after
// Close, or when the buffer is full, is dropped rather than blocking.
func (s *Store) RecordHTTPProbeAsync(p core.HTTPProbe) error {
	return s.enqueueProbe(probeWrite{probe: &p})
}

// RecordFullProbeJob persists a full-probe job snapshot. Progress snapshots
// (state "running") ride the batched, lossy probe pipeline — dropping one
// only loses a tick of progress. A terminal snapshot (done/canceled/error)
// is the job's final durable record and must NOT be droppable: if it were
// lost on a full buffer, the job would read as "running" forever after a
// restart (apply 409, cancel 409, endless UI polling). So terminal snapshots
// take a synchronous write, like exit-IP checks, guaranteeing they land.
// Snapshots are keyed by job ID, so a later snapshot overwrites the earlier.
func (s *Store) RecordFullProbeJob(j *core.FullProbeJob) error {
	if j == nil {
		return errors.New("metrics: nil full probe job")
	}
	if j.State == core.FullProbeRunning {
		return s.enqueueProbe(probeWrite{job: j})
	}
	if s.closed.Load() {
		return ErrClosed
	}
	val, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketFullProbes)
		v := val
		// A retention pass may already have marked the durable record's
		// rows pruned; the runner's snapshot never carries that mark, so
		// preserve it rather than clearing it on overwrite.
		if prev := fb.Get([]byte(j.ID)); prev != nil {
			var old core.FullProbeJob
			if json.Unmarshal(prev, &old) == nil && old.RowsPruned && !j.RowsPruned {
				marked := *j
				marked.RowsPruned = true
				if mv, merr := json.Marshal(&marked); merr == nil {
					v = mv
				}
			}
		}
		if err := fb.Put([]byte(j.ID), v); err != nil {
			return err
		}
		pruneFullProbeJobs(fb, tx.Bucket(bucketFullProbeRows), j.ID)
		return nil
	})
}

// enqueueProbe offers one probe row to the batched writer without blocking.
func (s *Store) enqueueProbe(w probeWrite) error {
	if s.probeClosed.Load() {
		return errors.New("metrics: store closed")
	}
	s.probeSendMu.RLock()
	defer s.probeSendMu.RUnlock()
	if s.probeClosed.Load() {
		return errors.New("metrics: store closed")
	}
	select {
	case s.probeIn <- w:
		return nil
	default:
		return errors.New("metrics: probe write buffer full, dropping record")
	}
}

// RecentHTTPProbes returns up to n HTTP probe records, newest first. When
// route is non-empty, only that route's records are returned.
func (s *Store) RecentHTTPProbes(n int, route string) ([]core.HTTPProbe, error) {
	if n < 1 {
		n = 1
	}
	out := make([]core.HTTPProbe, 0, n)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketProbes).Cursor()
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var p core.HTTPProbe
			if err := json.Unmarshal(v, &p); err == nil {
				if route != "" && p.Route != route {
					continue
				}
				out = append(out, p)
				if len(out) >= n {
					return nil
				}
			}
		}
		return nil
	})
	return out, err
}

// probeLoop drains the probe write channel and batch-inserts into bbolt,
// mirroring loop() but routing each row to its own bucket: HTTP probe records
// to bucketProbes under a time-ordered key, full-probe attempt rows (those
// carrying a FullProbeID) to their job's nested bucket under bucketFullProbeRows
// — successes dual-write to both, failures land only in the job bucket — and
// full-probe job progress snapshots to bucketFullProbes keyed by job ID (so a
// progress snapshot overwrites the previous one in place). Terminal job
// snapshots never come through here — RecordFullProbeJob writes them
// synchronously so they cannot be dropped.
func (s *Store) probeLoop() {
	defer close(s.probeDone)
	batch := make([]probeWrite, 0, probeBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		var delta int64
		var jobRows map[string]int64
		var fpTotal int64
		s.probeWriteMu.Lock()
		err := s.db.Batch(func(tx *bolt.Tx) error {
			// Batch may re-run fn in a fresh tx if a batch-mate fails:
			// re-read the shared baselines each attempt. probeWriteMu spans
			// the commit and the publish, so they equal the committed row
			// counts (see the probeRows field comment).
			base := s.probeRows.Load()
			rows := base
			jobRows = make(map[string]int64, len(s.fpJobRows))
			for id, n := range s.fpJobRows {
				jobRows[id] = n
			}
			fpTotal = s.fpRowsTotal.Load()
			pb := tx.Bucket(bucketProbes)
			fb := tx.Bucket(bucketFullProbes)
			fr := tx.Bucket(bucketFullProbeRows)
			for _, w := range batch {
				switch {
				case w.probe != nil:
					val, err := json.Marshal(w.probe)
					if err != nil {
						continue
					}
					if w.probe.FullProbeID != "" {
						// A full-probe attempt row belongs to its job's
						// bucket, keyed by the probe ID verbatim: the ID's
						// 19-digit nanosecond prefix orders attempts
						// chronologically, so the reader pages newest-first
						// and the prune drops oldest-first.
						jb, berr := fr.CreateBucketIfNotExists([]byte(w.probe.FullProbeID))
						if berr != nil {
							return berr
						}
						pkey := []byte(w.probe.ID)
						isNew := jb.Get(pkey) == nil
						if jb.Put(pkey, val) == nil && isNew {
							jobRows[w.probe.FullProbeID]++
							fpTotal++
						}
						if jobRows[w.probe.FullProbeID] > fullProbeRowsPerJob {
							kept := pruneTimeBucket(jb, jobRows[w.probe.FullProbeID], fullProbeRowsPerJob)
							fpTotal -= jobRows[w.probe.FullProbeID] - kept
							jobRows[w.probe.FullProbeID] = kept
							markFullProbeRowsPruned(fb, w.probe.FullProbeID)
						}
					}
					// The shared probe history keeps plain probe rounds and
					// the full-probe SUCCESS mirror only — failures would
					// push the successes a ProbesPage reader wants out of
					// the 20000-row window.
					if w.probe.FullProbeID == "" || w.probe.Success {
						key := []byte(fmt.Sprintf("%d-%s", w.probe.StartedAt.UnixNano(), w.probe.ID))
						isNew := pb.Get(key) == nil
						if pb.Put(key, val) == nil && isNew {
							rows++ // an overwrite Put (duplicate key) adds no row
						}
					}
				case w.job != nil:
					// A queued progress snapshot must not clobber a
					// terminal record that already landed synchronously:
					// the runner enqueues progress before its final
					// persist, and this buffer may flush after it.
					if prev := fb.Get([]byte(w.job.ID)); prev != nil {
						var old core.FullProbeJob
						if json.Unmarshal(prev, &old) == nil {
							if w.job.State == core.FullProbeRunning && old.State != core.FullProbeRunning {
								continue
							}
							// Likewise a retention mark on the durable
							// record must survive a later snapshot from the
							// runner, which never sees RowsPruned.
							if old.RowsPruned && !w.job.RowsPruned {
								w.job.RowsPruned = true
							}
						}
					}
					val, err := json.Marshal(w.job)
					if err != nil {
						continue
					}
					_ = fb.Put([]byte(w.job.ID), val)
				}
			}
			rows = pruneTimeBucket(pb, rows, probeHistory)
			delta = rows - base
			if len(jobRows) > fullProbeRowJobs || fpTotal > fullProbeRowsTotal {
				fpTotal = evictFullProbeRowBuckets(fr, fb, jobRows, fpTotal, true)
			}
			return nil
		})
		if err == nil {
			s.probeRows.Add(delta)
			s.fpJobRows = jobRows
			s.fpRowsTotal.Store(fpTotal)
		}
		s.probeWriteMu.Unlock()
		batch = batch[:0]
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case w, ok := <-s.probeIn:
			if !ok {
				flush()
				return
			}
			batch = append(batch, w)
			if len(batch) >= probeBatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

// pruneTimeBucket caps a time-ordered row bucket at max rows, deleting the
// oldest (keys sort ascending by time). count is the writer's running row
// count; the return value is the count after pruning. Called inside the
// flush transaction, so the cap holds without a separate scan pass, and the
// cursor walk only runs on flushes that cross the cap — amortized O(1) per
// row, not a per-flush scan of the whole bucket.
func pruneTimeBucket(b *bolt.Bucket, count int64, max int64) int64 {
	if count <= max {
		return count
	}
	excess := count - max
	deleted := int64(0)
	c := b.Cursor()
	for deleted < excess {
		// bbolt's documented contract: mutating the bucket invalidates a
		// cursor's position, and it must be repositioned before further
		// traversal. Re-seek First after every delete rather than advancing
		// a possibly-stale cursor, which can skip keys on a node-materialized
		// leaf and silently under-prune.
		k, _ := c.First()
		if k == nil {
			break
		}
		if b.Delete(k) != nil {
			break
		}
		deleted++
	}
	return count - deleted
}

// pruneFullProbeJobs keeps at most fullProbeHistory job snapshots, deleting
// the least recently active: FinishedAt when set (a terminal job), else
// StartedAt. Sorting by StartedAt alone would evict a long-running job's own
// terminal snapshot in the very transaction that landed it — it started
// long ago, so it sorted oldest even though it just finished. The landing
// job's ID is additionally exempt, so the snapshot being written can never
// be the one pruned. The count comes from the cursor, not Stats().KeyN:
// Stats walks spilled pages only, so keys staged by this very transaction
// are invisible to it and the guard would read a stale (often zero) count.
//
// Invariant: a snapshot is pruned from history ONLY when its row bucket is
// already gone, and never while the job runs. Two more exemptions enforce
// it: a running snapshot (a long-running job must not lose its live record
// after fullProbeHistory newer terminal jobs land), and any snapshot whose
// row bucket still exists in fr — deleting that snapshot would strand the
// bucket as a live eviction's "missing snapshot" case forever. The history
// is therefore bounded by fullProbeHistory + running jobs + jobs with row
// buckets (itself capped near fullProbeRowJobs), not exactly fullProbeHistory.
// With those exemptions, a live job whose bucket exists but whose snapshot
// is missing can only be an in-flight job whose first LOSSY progress
// snapshot has not landed yet — which is exactly what evictFullProbeRowBuckets'
// live mode protects.
func pruneFullProbeJobs(fb, fr *bolt.Bucket, landing string) {
	type jobKey struct {
		id   string
		at   time.Time
		live bool // running snapshot or surviving row bucket: never prune
	}
	var all []jobKey
	_ = fb.ForEach(func(k, v []byte) error {
		var j core.FullProbeJob
		if err := json.Unmarshal(v, &j); err == nil {
			at := j.StartedAt
			if j.FinishedAt.After(at) {
				at = j.FinishedAt
			}
			all = append(all, jobKey{
				id:   string(k),
				at:   at,
				live: j.State == core.FullProbeRunning || fr.Bucket(k) != nil,
			})
		}
		return nil
	})
	if len(all) <= fullProbeHistory {
		return
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	remove := len(all) - fullProbeHistory
	for _, j := range all {
		if remove == 0 {
			break
		}
		if j.id == landing || j.live {
			continue
		}
		_ = fb.Delete([]byte(j.id))
		remove--
	}
}

// markFullProbeRowsPruned sets RowsPruned on the job's durable snapshot by
// read-modify-write, preserving its State and every other field — a
// retention mark must never revert a terminal record or resurrect a running
// zombie. Called inside the write transaction that pruned or evicted the
// job's rows.
func markFullProbeRowsPruned(fb *bolt.Bucket, id string) {
	v := fb.Get([]byte(id))
	if v == nil {
		return
	}
	var j core.FullProbeJob
	if json.Unmarshal(v, &j) != nil || j.RowsPruned {
		return
	}
	j.RowsPruned = true
	if val, err := json.Marshal(&j); err == nil {
		_ = fb.Put([]byte(id), val)
	}
}

// evictFullProbeRowBuckets deletes whole job result buckets until the job
// count and the global row budget hold again, updating jobRows (the flush's
// per-job baseline copy) and returning the new total.
//
// Invariant B: a LIVE flush (live=true) deletes a bucket ONLY when a durable
// TERMINAL snapshot proves the job finished. A running snapshot exempts the
// bucket, and so does a MISSING or undecodable one: progress snapshots ride
// the lossy batched pipeline, so a live job's first snapshot may not have
// landed yet, and pruneFullProbeJobs (invariant A) never prunes a snapshot
// whose row bucket still exists — so nil-with-bucket can only mean an
// in-flight job, never a finished one. Treating nil as finished would evict
// a running job's whole bucket mid-sweep, and markFullProbeRowsPruned would
// no-op on the absent snapshot: silent row loss behind exact UI counts.
// At Open (live=false) nil is NOT ambiguous: reconcileFullProbeZombies has
// already landed every running snapshot as terminal and no runner exists in
// this process yet, so a missing/undecodable snapshot means a crashed job
// whose orphaned bucket must stay evictable.
//
// Skipping running/ambiguous jobs may leave the budget exceeded. The
// overshoot is bounded by the running jobs (at most FullProbeMaxRunningJobs)
// PLUS exited jobs whose terminal persist is still retrying: the router
// retries with backoff until the record lands or the store closes. A store
// that never accepts the write again keeps those buckets exempt until
// restart, when reconcileFullProbeZombies terminalizes them.
// Eviction order is lexicographic by job ID: IDs are minted
// "fp-<19-digit UnixNano>-<seq>" with no '/', so lexicographic order is
// chronological — oldest first. Each evicted job's snapshot is marked
// RowsPruned so the UI can say the detail is partial, not that the job never
// ran those attempts.
func evictFullProbeRowBuckets(fr, fb *bolt.Bucket, jobRows map[string]int64, total int64, live bool) int64 {
	if len(jobRows) <= fullProbeRowJobs && total <= fullProbeRowsTotal {
		return total
	}
	ids := make([]string, 0, len(jobRows))
	for id := range jobRows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if len(jobRows) <= fullProbeRowJobs && total <= fullProbeRowsTotal {
			break
		}
		if v := fb.Get([]byte(id)); v != nil {
			var j core.FullProbeJob
			if json.Unmarshal(v, &j) != nil {
				// Undecodable: no proof of completion while live; at
				// Open it is corrupt debris from a dead process.
				if live {
					continue
				}
			} else if j.State == core.FullProbeRunning {
				continue
			}
		} else if live {
			// No durable snapshot yet: the job may still be running
			// (its first lossy progress snapshot has not landed).
			continue
		}
		if fr.DeleteBucket([]byte(id)) != nil {
			continue
		}
		total -= jobRows[id]
		delete(jobRows, id)
		markFullProbeRowsPruned(fb, id)
	}
	return total
}

// FullProbeResultFilter narrows a job's attempt results. Every field is
// optional; the zero value matches all rows.
type FullProbeResultFilter struct {
	// Outcome is "success" or "failed"; empty matches both.
	Outcome string
	// Depth is the exact chain length to match (valid depths are >= 1);
	// zero means unset.
	Depth int
	// Reason matches the failure's classified reason slug exactly.
	Reason string
	// Route matches the attempt's hop-join route ("a>b") exactly.
	Route string
	// Hop matches an outbound appearing at ANY position of the route.
	Hop string
	// LastHop matches the route's final outbound.
	LastHop string
}

// FullProbeResults returns up to n of one job's durable attempt rows, newest
// first, narrowed by f. before is a probe ID cursor: only rows strictly older
// (lexicographically smaller keys) are returned, so a page's last row ID
// pages past itself. Seek lands on the first key >= before even when that
// exact row was pruned, so pagination stays correct across retention cuts.
// A job with no result bucket (unknown, evicted, or no issued attempts)
// yields an empty slice, not an error.
func (s *Store) FullProbeResults(jobID string, n int, before string, f FullProbeResultFilter) ([]core.HTTPProbe, error) {
	if n < 1 {
		n = 1
	}
	out := make([]core.HTTPProbe, 0, n)
	err := s.db.View(func(tx *bolt.Tx) error {
		jb := tx.Bucket(bucketFullProbeRows).Bucket([]byte(jobID))
		if jb == nil {
			return nil
		}
		c := jb.Cursor()
		var k, v []byte
		if before == "" {
			k, v = c.Last()
		} else {
			// Seek positions at the first key >= before (or past the end,
			// where Prev yields the last key); Prev then steps to the first
			// key strictly below before — the cursor row itself is excluded.
			c.Seek([]byte(before))
			k, v = c.Prev()
		}
		for ; k != nil; k, v = c.Prev() {
			// Two-stage decode: unmarshal only the predicate fields first,
			// so a filtered page over a 500000-row job does not materialize
			// every row's headers, body and path graph.
			var head struct {
				Success bool   `json:"success"`
				Depth   int    `json:"depth,omitempty"`
				Reason  string `json:"reason,omitempty"`
				Route   string `json:"route"`
			}
			if json.Unmarshal(v, &head) != nil {
				continue
			}
			switch f.Outcome {
			case "success":
				if !head.Success {
					continue
				}
			case "failed":
				if head.Success {
					continue
				}
			}
			if f.Depth != 0 && head.Depth != f.Depth {
				continue
			}
			if f.Reason != "" && head.Reason != f.Reason {
				continue
			}
			if f.Route != "" && head.Route != f.Route {
				continue
			}
			if f.Hop != "" || f.LastHop != "" {
				hops := strings.Split(head.Route, ">")
				if f.Hop != "" {
					found := false
					for _, h := range hops {
						if h == f.Hop {
							found = true
							break
						}
					}
					if !found {
						continue
					}
				}
				if f.LastHop != "" && hops[len(hops)-1] != f.LastHop {
					continue
				}
			}
			var p core.HTTPProbe
			if err := json.Unmarshal(v, &p); err == nil {
				out = append(out, p)
				if len(out) >= n {
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FullProbeJob returns the persisted snapshot of one full-probe job by ID.
// The runner serves live jobs from memory; this is the durable fallback for a
// job that has finished (or an engine that restarted mid-history).
func (s *Store) FullProbeJob(id string) (*core.FullProbeJob, bool) {
	var out *core.FullProbeJob
	_ = s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketFullProbes).Get([]byte(id))
		if v == nil {
			return nil
		}
		var j core.FullProbeJob
		if err := json.Unmarshal(v, &j); err == nil {
			out = &j
		}
		return nil
	})
	return out, out != nil
}

// RecentFullProbeJobs returns up to n persisted full-probe job snapshots,
// newest first by StartedAt. Ordering decodes only each row's timestamp —
// a job can carry thousands of success paths, and the list must not
// materialize every history entry to return n of them.
func (s *Store) RecentFullProbeJobs(n int) ([]*core.FullProbeJob, error) {
	if n < 1 {
		n = 1
	}
	type stamp struct {
		id string
		at time.Time
	}
	var stamps []stamp
	var out []*core.FullProbeJob
	err := s.db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketFullProbes)
		_ = fb.ForEach(func(k, v []byte) error {
			var head struct {
				StartedAt time.Time `json:"startedAt"`
			}
			if err := json.Unmarshal(v, &head); err == nil {
				stamps = append(stamps, stamp{id: string(k), at: head.StartedAt})
			}
			return nil
		})
		sort.Slice(stamps, func(i, j int) bool { return stamps[i].at.After(stamps[j].at) })
		if len(stamps) > n {
			stamps = stamps[:n]
		}
		for _, st := range stamps {
			var j core.FullProbeJob
			if err := json.Unmarshal(fb.Get([]byte(st.id)), &j); err == nil {
				out = append(out, &j)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
