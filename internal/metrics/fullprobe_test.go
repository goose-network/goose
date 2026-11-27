package metrics

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestRecordHTTPProbeAsyncBatched asserts rows offered to the async probe
// pipeline land in bucketProbes without a synchronous write, carrying the
// full-probe linkage (FullProbeID/Depth) through the batch.
func TestRecordHTTPProbeAsyncBatched(t *testing.T) {
	s, err := Open(t.TempDir() + "/async.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	const n = 150 // > probeBatchSize, so at least one size-triggered flush runs
	base := time.Now()
	for i := 0; i < n; i++ {
		require.NoError(t, s.RecordHTTPProbeAsync(core.HTTPProbe{
			ID:          fmt.Sprintf("p%d", i),
			Route:       "outbound:relay>work",
			Target:      "http://example.com",
			Success:     true,
			Status:      200,
			StartedAt:   base.Add(time.Duration(i) * time.Millisecond),
			FullProbeID: "fp-x",
			Depth:       2,
		}))
	}
	require.Eventually(t, func() bool {
		rows, err := s.RecentHTTPProbes(n+10, "")
		require.NoError(t, err)
		return len(rows) == n
	}, 5*time.Second, 20*time.Millisecond, "async probe rows never all landed")

	rows, err := s.RecentHTTPProbes(n+10, "")
	require.NoError(t, err)
	assert.Equal(t, "fp-x", rows[0].FullProbeID)
	assert.Equal(t, 2, rows[0].Depth)
	assert.True(t, rows[0].Success)
}

// TestFullProbeJobSnapshotOverwrite asserts job snapshots are keyed by ID:
// a later snapshot (progress → terminal) replaces the earlier one in place,
// and the durable read serves the latest.
func TestFullProbeJobSnapshotOverwrite(t *testing.T) {
	s, err := Open(t.TempDir() + "/job.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-1", State: core.FullProbeRunning, StartedAt: start,
		Levels: []core.FullProbeLevel{{Depth: 1, Done: 3}},
	}))
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-1", State: core.FullProbeDone, StartedAt: start, FinishedAt: start.Add(time.Second),
		Levels: []core.FullProbeLevel{{Depth: 1, Done: 10, Success: 7,
			SuccessPaths: []core.FullProbePath{{Hops: []string{"relay", "work"}, Status: 200}}}},
	}))

	require.Eventually(t, func() bool {
		j, ok := s.FullProbeJob("fp-1")
		return ok && j.State == core.FullProbeDone
	}, 5*time.Second, 20*time.Millisecond, "terminal snapshot never overwrote the running one")

	j, ok := s.FullProbeJob("fp-1")
	require.True(t, ok)
	require.Len(t, j.Levels, 1)
	assert.Equal(t, 7, j.Levels[0].Success)
	require.Len(t, j.Levels[0].SuccessPaths, 1)
	assert.Equal(t, []string{"relay", "work"}, j.Levels[0].SuccessPaths[0].Hops)

	all, err := s.RecentFullProbeJobs(10)
	require.NoError(t, err)
	require.Len(t, all, 1, "overwrite must not create a second history entry")

	_, ok = s.FullProbeJob("fp-absent")
	assert.False(t, ok)
	assert.Error(t, s.RecordFullProbeJob(nil))
}

// TestFullProbeHistoryPruned asserts terminal snapshots prune the history to
// fullProbeHistory entries, dropping the oldest by StartedAt.
func TestFullProbeHistoryPruned(t *testing.T) {
	s, err := Open(t.TempDir() + "/prune.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	const total = fullProbeHistory + 8
	base := time.Now().Add(-time.Hour)
	for i := 0; i < total; i++ {
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID:        fmt.Sprintf("fp-%03d", i),
			State:     core.FullProbeDone,
			StartedAt: base.Add(time.Duration(i) * time.Second),
		}))
	}
	// The first 64-row batch already equals the cap, so wait for the prune
	// itself (oldest gone), not merely for a 64-long history.
	require.Eventually(t, func() bool {
		_, oldest := s.FullProbeJob("fp-000")
		all, err := s.RecentFullProbeJobs(total + 10)
		require.NoError(t, err)
		return !oldest && len(all) == fullProbeHistory
	}, 5*time.Second, 20*time.Millisecond, "history never pruned to the cap")

	all, err := s.RecentFullProbeJobs(total + 10)
	require.NoError(t, err)
	// Newest first: the last-written job survives, the oldest were dropped.
	assert.Equal(t, fmt.Sprintf("fp-%03d", total-1), all[0].ID)
	_, ok := s.FullProbeJob("fp-000")
	assert.False(t, ok, "oldest job should have been pruned")
}

// TestCloseFlushesProbeWrites asserts Close drains the probe pipeline: a
// snapshot enqueued immediately before Close is durable after it returns.
func TestCloseFlushesProbeWrites(t *testing.T) {
	path := t.TempDir() + "/flush.db"
	s, err := Open(path)
	require.NoError(t, err)

	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-flush", State: core.FullProbeDone, StartedAt: time.Now(),
	}))
	require.NoError(t, s.RecordHTTPProbeAsync(core.HTTPProbe{
		ID: "p-flush", Target: "http://example.com", StartedAt: time.Now(), Success: true,
	}))
	require.NoError(t, s.Close())

	s2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	j, ok := s2.FullProbeJob("fp-flush")
	require.True(t, ok, "Close must flush pending job snapshots")
	assert.Equal(t, core.FullProbeDone, j.State)
	rows, err := s2.RecentHTTPProbes(10, "")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "p-flush", rows[0].ID)
}

// TestProbeEnqueueDuringCloseNoRace hammers the async probe writers
// concurrently with Close: the probeSendMu + probeClosed guard must prevent
// send-on-closed-channel panics and races, mirroring the request pipeline's
// regression test.
func TestProbeEnqueueDuringCloseNoRace(t *testing.T) {
	s, err := Open(t.TempDir() + "/probe-race.db")
	require.NoError(t, err)

	const writers = 8
	const perWriter = 300
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				_ = s.RecordHTTPProbeAsync(core.HTTPProbe{
					ID: fmt.Sprintf("p-%d-%d", id, j), StartedAt: time.Now(),
				})
				_ = s.RecordFullProbeJob(&core.FullProbeJob{
					ID: fmt.Sprintf("fp-%d", id), State: core.FullProbeRunning, StartedAt: time.Now(),
				})
			}
		}(i)
	}
	require.NoError(t, s.Close())
	wg.Wait()

	// After Close the writers report the closed-store error, never panic.
	assert.Error(t, s.RecordHTTPProbeAsync(core.HTTPProbe{ID: "late"}))
	assert.Error(t, s.RecordFullProbeJob(&core.FullProbeJob{ID: "late"}))
}

// TestOpenReconcilesRunningZombies asserts a job persisted as "running" by a
// process that died is landed in a terminal state when the DB is reopened:
// otherwise it would read as running forever (apply 409, cancel 409, the UI
// polling endlessly). Finished jobs are untouched.
func TestOpenReconcilesRunningZombies(t *testing.T) {
	path := t.TempDir() + "/zombie.db"
	s, err := Open(path)
	require.NoError(t, err)
	start := time.Now().Add(-time.Hour)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-zombie", State: core.FullProbeRunning, StartedAt: start,
		Levels: []core.FullProbeLevel{{Depth: 1, Done: 3, Total: 10}},
	}))
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-done", State: core.FullProbeDone, StartedAt: start, FinishedAt: start.Add(time.Minute),
	}))
	require.Eventually(t, func() bool {
		_, ok := s.FullProbeJob("fp-zombie")
		return ok
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s.Close())

	s2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	j, ok := s2.FullProbeJob("fp-zombie")
	require.True(t, ok)
	assert.Equal(t, core.FullProbeError, j.State, "restarted runner's job must not stay running")
	assert.NotEmpty(t, j.Error)
	assert.False(t, j.FinishedAt.IsZero())
	require.Len(t, j.Levels, 1, "partial progress survives the reconciliation")
	assert.Equal(t, 3, j.Levels[0].Done)
	d, ok := s2.FullProbeJob("fp-done")
	require.True(t, ok)
	assert.Equal(t, core.FullProbeDone, d.State, "finished jobs are untouched")
}

// TestTerminalSnapshotNotDropped asserts the terminal snapshot is durable
// the instant RecordFullProbeJob returns — it must not ride the lossy
// drop-on-full probe buffer, or a full buffer at high rps could strand the
// job as "running" in history.
func TestTerminalSnapshotNotDropped(t *testing.T) {
	s, err := Open(t.TempDir() + "/terminal.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// Saturate the async buffer with probe rows so a lossy enqueue of the
	// terminal snapshot would certainly drop.
	for i := 0; i < probeWriteBuf+1024; i++ {
		_ = s.RecordHTTPProbeAsync(core.HTTPProbe{ID: fmt.Sprintf("flood-%d", i), StartedAt: time.Now()})
	}
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-terminal", State: core.FullProbeDone, StartedAt: time.Now(), FinishedAt: time.Now(),
	}))
	j, ok := s.FullProbeJob("fp-terminal")
	require.True(t, ok, "terminal snapshot must be readable without waiting for a flush")
	assert.Equal(t, core.FullProbeDone, j.State)
}

// TestRunningSnapshotCannotClobberTerminal asserts a queued progress
// snapshot flushing after the synchronous terminal write does not revert
// the durable record to "running".
func TestRunningSnapshotCannotClobberTerminal(t *testing.T) {
	path := t.TempDir() + "/clobber.db"
	s, err := Open(path)
	require.NoError(t, err)

	start := time.Now()
	// Queue progress snapshots until at least one is accepted (the buffer
	// may be full). The terminal write below lands synchronously while
	// accepted progress rows may still sit in the buffer; their later
	// flush must not clobber it.
	queued := false
	for i := 0; i < probeWriteBuf+2048; i++ {
		if s.RecordFullProbeJob(&core.FullProbeJob{
			ID: "fp-race", State: core.FullProbeRunning, StartedAt: start,
			Levels: []core.FullProbeLevel{{Depth: 1, Done: i}},
		}) == nil {
			queued = true
		}
	}
	require.True(t, queued)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-race", State: core.FullProbeDone, StartedAt: start, FinishedAt: time.Now(),
		Levels: []core.FullProbeLevel{{Depth: 1, Done: 10, Success: 10}},
	}))
	// Drain everything (Close flushes the pipeline), then reopen: the
	// durable record must still be terminal.
	require.NoError(t, s.Close())
	s2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	j, ok := s2.FullProbeJob("fp-race")
	require.True(t, ok)
	assert.Equal(t, core.FullProbeDone, j.State, "queued progress must not clobber the terminal record")
}

// TestFullProbePruneKeepsLandingJob asserts a long-running job's terminal
// snapshot survives the prune its own landing triggers: sorting by start
// time alone would evict it as "oldest" even though it just finished.
func TestFullProbePruneKeepsLandingJob(t *testing.T) {
	s, err := Open(t.TempDir() + "/landing.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	now := time.Now()
	// The old job started long ago and finishes now.
	oldStart := now.Add(-2 * time.Hour)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-old", State: core.FullProbeRunning, StartedAt: oldStart,
	}))
	// Fill the history with jobs that started after it and finished before it.
	for i := 0; i < fullProbeHistory; i++ {
		st := oldStart.Add(time.Duration(i+1) * time.Minute)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: fmt.Sprintf("fp-mid-%03d", i), State: core.FullProbeDone,
			StartedAt: st, FinishedAt: st.Add(time.Second),
		}))
	}
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-old", State: core.FullProbeDone, StartedAt: oldStart, FinishedAt: now,
	}))
	j, ok := s.FullProbeJob("fp-old")
	require.True(t, ok, "the landing terminal snapshot must never prune itself")
	assert.Equal(t, core.FullProbeDone, j.State)
	all, err := s.RecentFullProbeJobs(fullProbeHistory + 10)
	require.NoError(t, err)
	assert.Len(t, all, fullProbeHistory)
	_, ok = s.FullProbeJob("fp-mid-000")
	assert.False(t, ok, "least recently active job should have been pruned instead")
}

// TestRowRetentionCaps asserts the request and probe row buckets are pruned
// to their retention caps: a 10000 rps sweep must not grow the DB unbounded.
func TestRowRetentionCaps(t *testing.T) {
	path := t.TempDir() + "/retention.db"
	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now()
	const n = requestHistory + 500
	// The pipelines are lossy under backpressure; retry until every row is
	// accepted so the test measures pruning, not drops.
	offer := func(f func() error) {
		for f() != nil {
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < n; i++ {
		m := core.RequestMetric{ID: fmt.Sprintf("r%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond), Chain: []string{"relay"}, Success: true}
		offer(func() error { return s.Record(m) })
		p := core.HTTPProbe{ID: fmt.Sprintf("p%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond), Success: true}
		offer(func() error { return s.RecordHTTPProbeAsync(p) })
	}
	// Wait for both pipelines to fully drain (newest rows readable) and
	// prune: waiting on count<=cap alone could fire mid-drain before the
	// buckets ever crossed the cap.
	require.Eventually(t, func() bool {
		rows, err := s.Recent(1)
		if err != nil || len(rows) != 1 || rows[0].ID != fmt.Sprintf("r%06d", n-1) {
			return false
		}
		probes, err := s.RecentHTTPProbes(1, "")
		return err == nil && len(probes) == 1 && probes[0].ID == fmt.Sprintf("p%06d", n-1)
	}, 15*time.Second, 50*time.Millisecond, "row pipelines never drained")

	var reqCount, probeCount int
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketRequests).ForEach(func(k, _ []byte) error { reqCount++; return nil })
		_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { probeCount++; return nil })
		return nil
	}))
	assert.LessOrEqual(t, reqCount, requestHistory, "request rows must be capped")
	assert.LessOrEqual(t, probeCount, probeHistory, "probe rows must be capped")
	assert.Greater(t, reqCount, requestHistory-1000, "prune must not overshoot the cap")
	assert.Greater(t, probeCount, probeHistory-1000, "prune must not overshoot the cap")
	// Newest rows survive; the oldest were dropped.
	rows, err := s.Recent(1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fmt.Sprintf("r%06d", n-1), rows[0].ID)
	probes, err := s.RecentHTTPProbes(1, "")
	require.NoError(t, err)
	require.Len(t, probes, 1)
	assert.Equal(t, fmt.Sprintf("p%06d", n-1), probes[0].ID)
}

// TestSyncProbeRetentionCap asserts the synchronous RecordHTTPProbe path —
// the periodic probe rounds' writer — also enforces the probe-row cap.
// bucketProbes has two writers (this sync path and probeLoop); if only the
// async flush counted and pruned, periodic rounds would grow the bucket
// without bound and drift the shared counter below the true row count.
// The bucket is seeded near the cap through the fast async pipeline, then
// a handful of sync writes cross it — measuring the sync accounting
// without paying tens of thousands of fsyncs.
func TestSyncProbeRetentionCap(t *testing.T) {
	s, err := Open(t.TempDir() + "/sync-retention.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now()
	offer := func(f func() error) {
		for f() != nil {
			time.Sleep(time.Millisecond)
		}
	}
	const seed = probeHistory - 10
	for i := 0; i < seed; i++ {
		offer(func() error {
			return s.RecordHTTPProbeAsync(core.HTTPProbe{
				ID: fmt.Sprintf("a%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond), Success: true,
			})
		})
	}
	require.Eventually(t, func() bool {
		return s.probeRows.Load() == seed
	}, 10*time.Second, 25*time.Millisecond, "seed rows never drained")

	const extra = 500
	for i := 0; i < extra; i++ {
		require.NoError(t, s.RecordHTTPProbe(core.HTTPProbe{
			ID: fmt.Sprintf("s%06d", i), StartedAt: base.Add(time.Duration(seed+i) * time.Microsecond), Success: true,
		}))
	}
	var count int
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { count++; return nil })
		return nil
	}))
	assert.LessOrEqual(t, count, probeHistory, "sync probe rows must be capped")
	assert.Equal(t, probeHistory, count, "prune must delete exactly the excess")
	assert.Equal(t, int64(count), s.probeRows.Load(), "the shared counter must match the bucket")
	rows, err := s.RecentHTTPProbes(1, "")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fmt.Sprintf("s%06d", extra-1), rows[0].ID)
	// The oldest rows (async seeds) were the ones pruned.
	all, err := s.RecentHTTPProbes(probeHistory+10, "")
	require.NoError(t, err)
	for _, p := range all {
		assert.NotEqual(t, "a000000", p.ID, "the oldest seed row should have been pruned")
	}
}

// TestMixedProbeWritersKeepCounterExact hammers both bucketProbes writers
// concurrently: every write transaction publishes its delta after commit,
// so the shared counter must equal the true row count afterwards (bbolt
// serializes the transactions; the deltas sum exactly).
func TestMixedProbeWritersKeepCounterExact(t *testing.T) {
	s, err := Open(t.TempDir() + "/mixed.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	const n = 300
	base := time.Now()
	offer := func(f func() error) {
		for f() != nil {
			time.Sleep(time.Millisecond)
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			require.NoError(t, s.RecordHTTPProbe(core.HTTPProbe{
				ID: fmt.Sprintf("sync%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond),
			}))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			offer(func() error {
				return s.RecordHTTPProbeAsync(core.HTTPProbe{
					ID: fmt.Sprintf("async%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond),
				})
			})
		}
	}()
	wg.Wait()
	// Wait for the async pipeline to drain (its newest row readable).
	require.Eventually(t, func() bool {
		rows, err := s.RecentHTTPProbes(2*n, "")
		if err != nil {
			return false
		}
		for _, p := range rows {
			if p.ID == fmt.Sprintf("async%06d", n-1) {
				return true
			}
		}
		return false
	}, 10*time.Second, 25*time.Millisecond, "async probe rows never drained")

	var count int
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { count++; return nil })
		return nil
	}))
	assert.Equal(t, 2*n, count, "under the cap no row may be pruned")
	// The last flush's Add runs just after its commit becomes visible, so
	// counter equality is eventual, not instant.
	require.Eventually(t, func() bool {
		return s.probeRows.Load() == int64(count)
	}, 5*time.Second, 10*time.Millisecond, "mixed writers must keep the counter exact")
}

// TestProbeOverwritePutsNotCounted asserts that a Put onto an existing key —
// probe IDs are timestamp+route(+target) and can collide — adds no row to
// the shared counter, on both writers. Counting overwrites would inflate the
// counter permanently and over-prune live rows once the cap is reached.
func TestProbeOverwritePutsNotCounted(t *testing.T) {
	s, err := Open(t.TempDir() + "/overwrite.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	at := time.Now()
	p := core.HTTPProbe{ID: "dup", StartedAt: at, Success: true}
	for i := 0; i < 5; i++ {
		p.Status = 200 + i
		require.NoError(t, s.RecordHTTPProbe(p))
	}
	for i := 0; i < 5; i++ {
		p.Status = 300 + i
		require.NoError(t, s.RecordHTTPProbeAsync(p))
	}
	require.Eventually(t, func() bool {
		rows, err := s.RecentHTTPProbes(10, "")
		if err != nil || len(rows) != 1 {
			return false
		}
		return rows[0].Status == 304 // last async overwrite landed
	}, 5*time.Second, 10*time.Millisecond, "async overwrites never drained")

	var count int
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { count++; return nil })
		return nil
	}))
	assert.Equal(t, 1, count, "same key must stay one row")
	require.Eventually(t, func() bool {
		return s.probeRows.Load() == int64(count)
	}, 5*time.Second, 10*time.Millisecond, "overwrite Puts must not inflate the counter")
}

// TestMixedProbeWritersAtCap combines both writers crossing the retention
// cap with duplicate keys inside the async batch: the counter must still
// match the true row count exactly, and pruning must delete the oldest
// contiguous prefix (a stale cursor after Delete would skip alternate keys).
func TestMixedProbeWritersAtCap(t *testing.T) {
	s, err := Open(t.TempDir() + "/mixed-cap.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now()
	offer := func(f func() error) {
		for f() != nil {
			time.Sleep(time.Millisecond)
		}
	}
	const seed = probeHistory - 100
	for i := 0; i < seed; i++ {
		offer(func() error {
			return s.RecordHTTPProbeAsync(core.HTTPProbe{
				ID: fmt.Sprintf("a%06d", i), StartedAt: base.Add(time.Duration(i) * time.Microsecond),
			})
		})
	}
	require.Eventually(t, func() bool {
		return s.probeRows.Load() == seed
	}, 10*time.Second, 25*time.Millisecond, "seed rows never drained")

	const n = 300
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			require.NoError(t, s.RecordHTTPProbe(core.HTTPProbe{
				ID: fmt.Sprintf("sync%06d", i), StartedAt: base.Add(time.Duration(seed+i) * time.Microsecond),
			}))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			p := core.HTTPProbe{
				ID: fmt.Sprintf("async%06d", i), StartedAt: base.Add(time.Duration(seed+i) * time.Microsecond),
			}
			offer(func() error { return s.RecordHTTPProbeAsync(p) })
			// Same key and timestamp again: a duplicate within the batch
			// must not count as a second row.
			offer(func() error { return s.RecordHTTPProbeAsync(p) })
		}
	}()
	wg.Wait()
	require.Eventually(t, func() bool {
		rows, err := s.RecentHTTPProbes(2*n, "")
		if err != nil {
			return false
		}
		for _, p := range rows {
			if p.ID == fmt.Sprintf("async%06d", n-1) {
				return true
			}
		}
		return false
	}, 10*time.Second, 25*time.Millisecond, "async probe rows never drained")

	// Both writers are done; wait for the last flush's post-commit Add to
	// become visible, then make one final write so the bucket's final state
	// is settled by a prune decision on a fully published counter.
	countRows := func() int {
		var c int
		if err := s.db.View(func(tx *bolt.Tx) error {
			_ = tx.Bucket(bucketProbes).ForEach(func(k, _ []byte) error { c++; return nil })
			return nil
		}); err != nil {
			return -1
		}
		return c
	}
	require.Eventually(t, func() bool {
		return s.probeRows.Load() == int64(countRows())
	}, 5*time.Second, 10*time.Millisecond, "post-commit deltas never settled")
	require.NoError(t, s.RecordHTTPProbe(core.HTTPProbe{
		ID: "final", StartedAt: base.Add(time.Duration(seed+2*n) * time.Microsecond),
	}))

	var oldest string
	count := countRows()
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		if k, _ := tx.Bucket(bucketProbes).Cursor().First(); k != nil {
			oldest = string(k)
		}
		return nil
	}))
	assert.Equal(t, probeHistory, count, "prune must land exactly on the cap")
	assert.Equal(t, int64(count), s.probeRows.Load(), "at-cap mixed writers must keep the counter exact")
	// Pruning deletes a contiguous oldest prefix: seed+n+n distinct rows
	// plus the corrective one, capped at probeHistory, leaves a000501 the
	// oldest survivor — a stale cursor after Delete would skip alternate
	// keys and leave older rows behind.
	assert.Equal(t,
		fmt.Sprintf("%d-a%06d", base.Add(501*time.Microsecond).UnixNano(), 501),
		oldest, "prune must delete exactly the oldest excess rows")
}
