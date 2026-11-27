package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestFullProbeRowLiveEvictionNeedsTerminalProof asserts invariant B on the
// live flush path: under job-budget pressure a bucket survives unless a
// durable TERMINAL snapshot proves its job finished. A missing snapshot (the
// first lossy progress snapshot dropped) and a running snapshot both exempt
// the bucket; once the terminal snapshot lands, the bucket becomes evictable
// and the snapshot is marked RowsPruned.
func TestFullProbeRowLiveEvictionNeedsTerminalProof(t *testing.T) {
	s, err := Open(t.TempDir() + "/liveproof.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	// Oldest job: its rows landed but NO snapshot ever did — the lossy
	// pipeline dropped the first progress snapshot. Eviction must not read
	// the absence as "finished".
	offerProbe(t, s, core.HTTPProbe{
		ID: "missing-row", Route: "relay", Target: "http://example.com",
		StartedAt: start, FullProbeID: fpJobID(0), Depth: 1,
	})
	awaitJobRows(t, s, fpJobID(0), 1)

	// Second-oldest: a genuinely running job.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: fpJobID(1), State: core.FullProbeRunning, StartedAt: start,
	}))
	require.Eventually(t, func() bool {
		j, ok := s.FullProbeJob(fpJobID(1))
		return ok && j.State == core.FullProbeRunning
	}, 5*time.Second, 10*time.Millisecond, "running snapshot never landed")
	offerProbe(t, s, core.HTTPProbe{
		ID: "running-row", Route: "relay", Target: "http://example.com",
		StartedAt: start, FullProbeID: fpJobID(1), Depth: 1,
	})
	awaitJobRows(t, s, fpJobID(1), 1)

	// Nine finished jobs push the bucket count over the 8-job budget; each
	// landing flush evicts oldest FINISHED buckets, never the two unproven
	// ones — even though they sort oldest.
	for i := 2; i <= fullProbeRowJobs+2; i++ {
		id := fpJobID(i)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: id, State: core.FullProbeDone, StartedAt: start,
			FinishedAt: start.Add(time.Duration(i) * time.Second),
		}))
		offerProbe(t, s, core.HTTPProbe{
			ID: fmt.Sprintf("r-%s", id), Route: "relay", Target: "http://example.com",
			StartedAt: start, FullProbeID: id, Depth: 1,
		})
		awaitJobRows(t, s, id, 1)
	}

	require.Eventually(t, func() bool {
		s.probeWriteMu.Lock()
		defer s.probeWriteMu.Unlock()
		return len(s.fpJobRows) <= fullProbeRowJobs
	}, 10*time.Second, 10*time.Millisecond, "eviction never reached the job budget")

	// Both unproven buckets survive with their rows and counter entries.
	assert.Equal(t, 1, countJobRows(t, s, fpJobID(0)), "missing-snapshot bucket must survive live eviction")
	assert.Equal(t, 1, countJobRows(t, s, fpJobID(1)), "running bucket must survive live eviction")
	_, ok := s.FullProbeJob(fpJobID(0))
	assert.False(t, ok, "no snapshot existed; none may be invented")
	s.probeWriteMu.Lock()
	assert.Contains(t, s.fpJobRows, fpJobID(0))
	assert.Contains(t, s.fpJobRows, fpJobID(1))
	s.probeWriteMu.Unlock()
	// The oldest finished buckets were evicted and marked instead.
	for i := 2; i <= 4; i++ {
		assert.Equal(t, 0, countJobRows(t, s, fpJobID(i)))
		j, ok := s.FullProbeJob(fpJobID(i))
		require.True(t, ok)
		assert.True(t, j.RowsPruned)
	}

	// The missing-snapshot job finishes: its terminal snapshot lands
	// synchronously, making the bucket evictable on the next over-budget
	// flush (triggered by one more finished job's rows).
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: fpJobID(0), State: core.FullProbeDone, StartedAt: start, FinishedAt: start.Add(time.Hour),
	}))
	id := fpJobID(fullProbeRowJobs + 3)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start.Add(2 * time.Hour),
	}))
	offerProbe(t, s, core.HTTPProbe{
		ID: fmt.Sprintf("r-%s", id), Route: "relay", Target: "http://example.com",
		StartedAt: start, FullProbeID: id, Depth: 1,
	})
	awaitJobRows(t, s, id, 1)

	require.Eventually(t, func() bool {
		return countJobRows(t, s, fpJobID(0)) == 0
	}, 10*time.Second, 10*time.Millisecond, "terminal-proven bucket never evicted")
	j, ok := s.FullProbeJob(fpJobID(0))
	require.True(t, ok, "eviction drops rows, not the snapshot")
	assert.True(t, j.RowsPruned, "evicted job must be marked RowsPruned")
	assert.Equal(t, core.FullProbeDone, j.State)
	assert.Equal(t, 1, countJobRows(t, s, fpJobID(1)), "running bucket still protected")
	s.probeWriteMu.Lock()
	_, gone := s.fpJobRows[fpJobID(0)]
	var want int64
	for _, n := range s.fpJobRows {
		want += n
	}
	s.probeWriteMu.Unlock()
	assert.False(t, gone)
	assert.Equal(t, want, s.fpRowsTotal.Load(), "counters stay exact across the eviction")
}

// TestFullProbeHistoryPruneKeepsRunningAndBucketed asserts invariant A: the
// 64-snapshot history prune never deletes a running job's snapshot nor one
// whose row bucket still exists — those snapshots are the only proof live
// eviction can read, and deleting them would strand the bucket as an
// unevictable orphan. Bucket-less terminal snapshots beyond the cap still go.
func TestFullProbeHistoryPruneKeepsRunningAndBucketed(t *testing.T) {
	s, err := Open(t.TempDir() + "/histkeep.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now().Add(-time.Hour)
	// A long-running job older than the whole terminal flood: its snapshot
	// sorts oldest and must survive every prune the flood triggers.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-ancient-running", State: core.FullProbeRunning, StartedAt: base,
	}))
	require.Eventually(t, func() bool {
		j, ok := s.FullProbeJob("fp-ancient-running")
		return ok && j.State == core.FullProbeRunning
	}, 5*time.Second, 10*time.Millisecond, "running snapshot never landed")

	// A finished job whose row bucket still exists: history must keep its
	// snapshot until eviction removes the bucket.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-ancient-bucketed", State: core.FullProbeDone,
		StartedAt: base, FinishedAt: base.Add(time.Second),
	}))
	offerProbe(t, s, core.HTTPProbe{
		ID: "bucketed-row", Route: "relay", Target: "http://example.com",
		StartedAt: base, FullProbeID: "fp-ancient-bucketed", Depth: 1,
	})
	awaitJobRows(t, s, "fp-ancient-bucketed", 1)

	// Flood past the history cap with newer bucket-less terminal jobs.
	const flood = fullProbeHistory + 8
	for i := 0; i < flood; i++ {
		st := base.Add(time.Duration(i+2) * time.Minute)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: fpJobID(i), State: core.FullProbeDone, StartedAt: st, FinishedAt: st.Add(time.Second),
		}))
	}

	rj, ok := s.FullProbeJob("fp-ancient-running")
	require.True(t, ok, "running snapshot must survive the history prune")
	assert.Equal(t, core.FullProbeRunning, rj.State)
	bj, ok := s.FullProbeJob("fp-ancient-bucketed")
	require.True(t, ok, "snapshot with a surviving row bucket must survive")
	assert.Equal(t, core.FullProbeDone, bj.State)
	assert.Equal(t, 1, countJobRows(t, s, "fp-ancient-bucketed"), "history prune never touches rows")

	// The two exempt entries displace the oldest bucket-less terminal
	// snapshots; the rest of the history holds at the cap.
	pruned := flood + 2 - fullProbeHistory
	for i := 0; i < pruned; i++ {
		_, ok := s.FullProbeJob(fpJobID(i))
		assert.False(t, ok, "oldest bucket-less terminal snapshot pruned")
	}
	_, ok = s.FullProbeJob(fpJobID(pruned))
	assert.True(t, ok, "newest surviving terminal snapshot kept")
	_, ok = s.FullProbeJob(fpJobID(flood - 1))
	assert.True(t, ok, "the landing job's snapshot survives")
	all, err := s.RecentFullProbeJobs(flood + 10)
	require.NoError(t, err)
	assert.Len(t, all, fullProbeHistory, "history holds at cap with the two exempt entries")
}

// TestOpenPrunePublishesExactCounters asserts Open's one-shot prune publishes
// counters that EXACTLY equal the surviving buckets: the eviction runs on a
// copy inside the transaction and the live map/total are replaced only when
// the transaction commits.
func TestOpenPrunePublishesExactCounters(t *testing.T) {
	path := t.TempDir() + "/openexact.db"
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	start := time.Now()
	// Seed an over-budget DB directly: 12 finished buckets, varying row
	// counts, so a wrong publish cannot coincidentally match the total.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		fr, e := tx.CreateBucketIfNotExists(bucketFullProbeRows)
		if e != nil {
			return e
		}
		fb, e := tx.CreateBucketIfNotExists(bucketFullProbes)
		if e != nil {
			return e
		}
		for i := 0; i < fullProbeRowJobs+4; i++ {
			id := fpJobID(i)
			jb, e := fr.CreateBucketIfNotExists([]byte(id))
			if e != nil {
				return e
			}
			for k := 0; k <= i%3; k++ {
				if e := jb.Put([]byte(fmt.Sprintf("r%d", k)), []byte(`{"success":true}`)); e != nil {
					return e
				}
			}
			val, e := json.Marshal(&core.FullProbeJob{
				ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
			})
			if e != nil {
				return e
			}
			if e := fb.Put([]byte(id), val); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// Recount the real surviving buckets and require the published counters
	// to match them key-for-key.
	want := map[string]int64{}
	var wantTotal int64
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFullProbeRows).ForEachBucket(func(k []byte) error {
			var n int64
			_ = tx.Bucket(bucketFullProbeRows).Bucket(k).ForEach(func(_, _ []byte) error { n++; return nil })
			want[string(k)] = n
			wantTotal += n
			return nil
		})
	}))
	require.Len(t, want, fullProbeRowJobs, "Open prunes to the job budget once")
	s.probeWriteMu.Lock()
	assert.Equal(t, want, s.fpJobRows, "published per-job counters must equal the surviving buckets")
	s.probeWriteMu.Unlock()
	assert.Equal(t, wantTotal, s.fpRowsTotal.Load())
	for i := 0; i < 4; i++ {
		j, ok := s.FullProbeJob(fpJobID(i))
		require.True(t, ok, "eviction drops rows, not snapshots")
		assert.True(t, j.RowsPruned)
	}
}

// TestOpenPruneCommitFailureKeepsCounters asserts the one-shot prune never
// publishes counters for a transaction that did not commit: bbolt rolls back
// on failure, so the seeded (over-budget) baselines must survive untouched
// for the first flush to prune again. The rollback subtest runs the real tx
// body and THEN fails the commit — a closed DB fails in Begin before the body
// ever runs, so it alone cannot catch an in-tx mutation of the live counters.
func TestOpenPruneCommitFailureKeepsCounters(t *testing.T) {
	path := t.TempDir() + "/commitfail.db"
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	start := time.Now()
	seed := map[string]int64{}
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		fr, e := tx.CreateBucketIfNotExists(bucketFullProbeRows)
		if e != nil {
			return e
		}
		fb, e := tx.CreateBucketIfNotExists(bucketFullProbes)
		if e != nil {
			return e
		}
		for i := 0; i < fullProbeRowJobs+2; i++ {
			id := fpJobID(i)
			jb, e := fr.CreateBucketIfNotExists([]byte(id))
			if e != nil {
				return e
			}
			if e := jb.Put([]byte("r0"), []byte(`{"success":true}`)); e != nil {
				return e
			}
			val, e := json.Marshal(&core.FullProbeJob{
				ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
			})
			if e != nil {
				return e
			}
			if e := fb.Put([]byte(id), val); e != nil {
				return e
			}
			seed[id] = 1
		}
		return nil
	}))

	// Hand the helper the seeded baselines. No writer loops run on this
	// hand-built Store, so only the prune touches the counters.
	s := &Store{db: db, fpJobRows: map[string]int64{}}
	for id, n := range seed {
		s.fpJobRows[id] = n
	}
	s.fpRowsTotal.Store(int64(len(seed)))

	t.Run("rollback-after-eviction", func(t *testing.T) {
		// Run the real eviction inside the tx, then fail so bbolt rolls
		// back AFTER buckets were deleted in-tx. A tx body that published
		// the reduced counters itself would leave them below the surviving
		// (restored) rows — the exact regression the closed-DB case below
		// cannot reach, because Begin fails before the body runs.
		var evicted map[string]int64
		var evictedTotal int64
		err := s.db.Update(func(tx *bolt.Tx) error {
			var e error
			evicted, evictedTotal, e = s.pruneFullProbeRowsTx(tx)
			if e != nil {
				return e
			}
			return errors.New("simulated commit failure after eviction")
		})
		require.Error(t, err, "the post-eviction failure must surface")
		// The body really evicted inside the tx …
		assert.Len(t, evicted, fullProbeRowJobs, "tx body must evict over-budget jobs before the failure")
		assert.Equal(t, int64(fullProbeRowJobs), evictedTotal)
		// … but the rollback must leave the live counters at the seed.
		assert.Equal(t, seed, s.fpJobRows, "rolled-back tx must not publish the eviction copy")
		assert.Equal(t, int64(len(seed)), s.fpRowsTotal.Load(), "rolled-back tx must not reduce the total")
		var buckets int
		require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketFullProbeRows).ForEachBucket(func(_ []byte) error { buckets++; return nil })
		}))
		assert.Equal(t, fullProbeRowJobs+2, buckets, "rollback must restore every evicted bucket")
		j, ok := s.FullProbeJob(fpJobID(0))
		require.True(t, ok)
		assert.False(t, j.RowsPruned, "rollback must undo the in-tx RowsPruned mark")
	})

	t.Run("closed-db-begin-failure", func(t *testing.T) {
		// Begin fails before the tx body runs: the helper must propagate
		// the error and publish nothing.
		require.NoError(t, db.Close())
		require.Error(t, s.pruneFullProbeRowsAtOpen(), "a closed DB must fail the prune tx")
		assert.Equal(t, seed, s.fpJobRows, "failed Begin must not publish the eviction copy")
		assert.Equal(t, int64(len(seed)), s.fpRowsTotal.Load(), "failed Begin must not reduce the total")
	})

	t.Run("recovery", func(t *testing.T) {
		// With a usable DB the same helper prunes and publishes exact
		// counters, and the failed attempts left every bucket intact.
		db2, err := bolt.Open(path, 0o600, nil)
		require.NoError(t, err)
		var buckets int
		require.NoError(t, db2.View(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketFullProbeRows).ForEachBucket(func(_ []byte) error { buckets++; return nil })
		}))
		assert.Equal(t, fullProbeRowJobs+2, buckets, "no bucket may vanish on a failed commit")
		s.db = db2
		require.NoError(t, s.pruneFullProbeRowsAtOpen())
		assert.Len(t, s.fpJobRows, fullProbeRowJobs)
		assert.Equal(t, int64(fullProbeRowJobs), s.fpRowsTotal.Load())
		require.NoError(t, db2.Close())
	})
}

// TestOpenEvictsOrphanBucketWithoutSnapshot asserts a crash orphan — rows
// landed but the process died before ANY snapshot did — is evictable at Open
// (live=false): no runner exists in this process and reconcileFullProbeZombies
// had nothing to reconcile, so the missing snapshot proves a dead job, not an
// in-flight one. Otherwise live eviction would skip the bucket forever.
func TestOpenEvictsOrphanBucketWithoutSnapshot(t *testing.T) {
	path := t.TempDir() + "/orphan.db"
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	start := time.Now()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		fr, e := tx.CreateBucketIfNotExists(bucketFullProbeRows)
		if e != nil {
			return e
		}
		fb, e := tx.CreateBucketIfNotExists(bucketFullProbes)
		if e != nil {
			return e
		}
		// The orphan: oldest ID, rows but no snapshot at all.
		jb, e := fr.CreateBucketIfNotExists([]byte(fpJobID(0)))
		if e != nil {
			return e
		}
		if e := jb.Put([]byte("orphan-row"), []byte(`{"success":true}`)); e != nil {
			return e
		}
		// Enough finished jobs to cross the job budget.
		for i := 1; i <= fullProbeRowJobs+1; i++ {
			id := fpJobID(i)
			jb, e := fr.CreateBucketIfNotExists([]byte(id))
			if e != nil {
				return e
			}
			if e := jb.Put([]byte("r0"), []byte(`{"success":true}`)); e != nil {
				return e
			}
			val, e := json.Marshal(&core.FullProbeJob{
				ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
			})
			if e != nil {
				return e
			}
			if e := fb.Put([]byte(id), val); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	assert.Equal(t, 0, countJobRows(t, s, fpJobID(0)), "crash orphan must be evicted at open")
	s.probeWriteMu.Lock()
	_, present := s.fpJobRows[fpJobID(0)]
	assert.Len(t, s.fpJobRows, fullProbeRowJobs)
	s.probeWriteMu.Unlock()
	assert.False(t, present)
	assert.Equal(t, int64(fullProbeRowJobs), s.fpRowsTotal.Load(), "counters exact after orphan eviction")
	_, ok := s.FullProbeJob(fpJobID(0))
	assert.False(t, ok, "no snapshot existed to mark or keep")
}
