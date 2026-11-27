package metrics

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// fpJobID mints a test job ID with the production shape
// (fp-<19-digit ns>-<seq>): '/'-free, lexicographically chronological.
func fpJobID(i int) string {
	return fmt.Sprintf("fp-%019d-1", i)
}

// offerProbe retries a lossy enqueue until accepted, so retention tests
// measure pruning, not backpressure drops.
func offerProbe(t *testing.T, s *Store, p core.HTTPProbe) {
	t.Helper()
	for s.RecordHTTPProbeAsync(p) != nil {
		time.Sleep(time.Millisecond)
	}
}

// awaitJobRows waits until the async pipeline has published n rows for a job.
func awaitJobRows(t *testing.T, s *Store, jobID string, n int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.probeWriteMu.Lock()
		defer s.probeWriteMu.Unlock()
		return s.fpJobRows[jobID] == n
	}, 10*time.Second, 10*time.Millisecond, "job %s rows never drained to %d", jobID, n)
}

// countJobRows counts a job bucket's real keys.
func countJobRows(t *testing.T, s *Store, jobID string) int {
	t.Helper()
	var n int
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		jb := tx.Bucket(bucketFullProbeRows).Bucket([]byte(jobID))
		if jb != nil {
			_ = jb.ForEach(func(_, _ []byte) error { n++; return nil })
		}
		return nil
	}))
	return n
}

// TestFullProbeRowRouting asserts the flush's routing: a full-probe SUCCESS
// dual-writes (job bucket + shared probe history), a FAILURE lands only in
// the job bucket, and a plain probe row (no FullProbeID) only in the shared
// history. The job bucket keys rows by probe ID verbatim.
func TestFullProbeRowRouting(t *testing.T) {
	s, err := Open(t.TempDir() + "/routing.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now()
	ok := core.HTTPProbe{
		ID:    fmt.Sprintf("%d-relay-http://example.com", base.UnixNano()),
		Route: "relay", Target: "http://example.com", Success: true, Status: 200,
		StartedAt: base, FullProbeID: "fp-route", Depth: 1,
	}
	bad := core.HTTPProbe{
		ID:    fmt.Sprintf("%d-work-http://example.com", base.Add(time.Millisecond).UnixNano()),
		Route: "work", Target: "http://example.com", Success: false,
		Error: "connect refused", Phase: "connect", Reason: "connect_refused",
		StartedAt: base.Add(time.Millisecond), FullProbeID: "fp-route", Depth: 1,
	}
	plain := core.HTTPProbe{
		ID:    fmt.Sprintf("%d-outbound:relay-http://example.com", base.Add(2*time.Millisecond).UnixNano()),
		Route: "outbound:relay", Target: "http://example.com", Success: true,
		StartedAt: base.Add(2 * time.Millisecond),
	}
	offerProbe(t, s, ok)
	offerProbe(t, s, bad)
	offerProbe(t, s, plain)
	awaitJobRows(t, s, "fp-route", 2)

	// Job bucket: both attempts, keyed by probe ID verbatim.
	rows, err := s.FullProbeResults("fp-route", 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, bad.ID, rows[0].ID, "newest first")
	assert.False(t, rows[0].Success)
	assert.Equal(t, "connect_refused", rows[0].Reason)
	assert.Equal(t, ok.ID, rows[1].ID)
	assert.True(t, rows[1].Success)
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		jb := tx.Bucket(bucketFullProbeRows).Bucket([]byte("fp-route"))
		require.NotNil(t, jb)
		assert.NotNil(t, jb.Get([]byte(ok.ID)), "row key must be the probe ID verbatim")
		assert.NotNil(t, jb.Get([]byte(bad.ID)))
		return nil
	}))

	// Shared history: the success mirror and the plain row, NOT the failure.
	shared, err := s.RecentHTTPProbes(10, "")
	require.NoError(t, err)
	require.Len(t, shared, 2)
	for _, p := range shared {
		assert.NotEqual(t, bad.ID, p.ID, "failures must not enter the shared probe history")
	}

	// A failure-only job never touches the shared history at all.
	offerProbe(t, s, core.HTTPProbe{
		ID: "fail-only", Route: "dead", Target: "http://example.com",
		StartedAt: base, FullProbeID: "fp-failonly", Depth: 1, Reason: "connect_timeout",
	})
	awaitJobRows(t, s, "fp-failonly", 1)
	rows, err = s.FullProbeResults("fp-failonly", 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	shared, err = s.RecentHTTPProbes(10, "")
	require.NoError(t, err)
	assert.Len(t, shared, 2, "failure-only job must add no shared row")

	// Counters exact: two job buckets, three rows total, two shared.
	s.probeWriteMu.Lock()
	assert.Equal(t, int64(2), s.fpJobRows["fp-route"])
	assert.Equal(t, int64(1), s.fpJobRows["fp-failonly"])
	s.probeWriteMu.Unlock()
	assert.Equal(t, int64(3), s.fpRowsTotal.Load())
	assert.Equal(t, int64(2), s.probeRows.Load())
}

// TestFullProbeResultsFiltersAndCursor exercises the reader's server-side
// filters and the strictly-exclusive, prune-robust before cursor.
func TestFullProbeResultsFiltersAndCursor(t *testing.T) {
	s, err := Open(t.TempDir() + "/reader.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// Five rows with ascending 19-digit ns prefixes (chronological keys):
	//   r1 relay        success depth 1
	//   r2 work         failed  depth 1 connect_refused
	//   r3 relay>work   success depth 2
	//   r4 relay>dead   failed  depth 2 proxy_timeout
	//   r5 work>relay   failed  depth 2 connect_refused
	mk := func(i int, route string, success bool, depth int, reason string) core.HTTPProbe {
		return core.HTTPProbe{
			ID:    fmt.Sprintf("%019d-%s-http://example.com", i, route),
			Route: route, Target: "http://example.com", Success: success, Depth: depth,
			Reason: reason, StartedAt: time.Now(), FullProbeID: "fp-read",
		}
	}
	rows := []core.HTTPProbe{
		mk(1, "relay", true, 1, ""),
		mk(2, "work", false, 1, "connect_refused"),
		mk(3, "relay>work", true, 2, ""),
		mk(4, "relay>dead", false, 2, "proxy_timeout"),
		mk(5, "work>relay", false, 2, "connect_refused"),
	}
	for _, p := range rows {
		offerProbe(t, s, p)
	}
	awaitJobRows(t, s, "fp-read", 5)

	ids := func(got []core.HTTPProbe) []string {
		out := make([]string, 0, len(got))
		for _, p := range got {
			out = append(out, p.ID)
		}
		return out
	}
	query := func(n int, before string, f FullProbeResultFilter) []string {
		got, err := s.FullProbeResults("fp-read", n, before, f)
		require.NoError(t, err)
		return ids(got)
	}

	// Unfiltered: newest first, n honored.
	assert.Equal(t, []string{rows[4].ID, rows[3].ID, rows[2].ID, rows[1].ID, rows[0].ID},
		query(10, "", FullProbeResultFilter{}))
	assert.Equal(t, []string{rows[4].ID, rows[3].ID}, query(2, "", FullProbeResultFilter{}))

	// Outcome.
	assert.Equal(t, []string{rows[2].ID, rows[0].ID},
		query(10, "", FullProbeResultFilter{Outcome: "success"}))
	assert.Equal(t, []string{rows[4].ID, rows[3].ID, rows[1].ID},
		query(10, "", FullProbeResultFilter{Outcome: "failed"}))
	// Depth exact; 0 means unset.
	assert.Equal(t, []string{rows[4].ID, rows[3].ID, rows[2].ID},
		query(10, "", FullProbeResultFilter{Depth: 2}))
	assert.Len(t, query(10, "", FullProbeResultFilter{Depth: 3}), 0)
	// Reason exact.
	assert.Equal(t, []string{rows[4].ID, rows[1].ID},
		query(10, "", FullProbeResultFilter{Reason: "connect_refused"}))
	// Route exact.
	assert.Equal(t, []string{rows[2].ID},
		query(10, "", FullProbeResultFilter{Route: "relay>work"}))
	// Hop at any position; LastHop final only.
	assert.Equal(t, []string{rows[4].ID, rows[2].ID, rows[1].ID},
		query(10, "", FullProbeResultFilter{Hop: "work"}))
	assert.Equal(t, []string{rows[2].ID, rows[1].ID},
		query(10, "", FullProbeResultFilter{LastHop: "work"}))
	assert.Equal(t, []string{rows[3].ID},
		query(10, "", FullProbeResultFilter{Hop: "dead"}))
	// Combined predicates.
	assert.Equal(t, []string{rows[4].ID},
		query(10, "", FullProbeResultFilter{Outcome: "failed", Depth: 2, Reason: "connect_refused", Hop: "work"}))

	// Cursor: strictly exclusive of the named row.
	assert.Equal(t, []string{rows[2].ID, rows[1].ID, rows[0].ID},
		query(10, rows[3].ID, FullProbeResultFilter{}))
	assert.Len(t, query(10, rows[0].ID, FullProbeResultFilter{}), 0, "nothing older than the oldest row")
	// A pruned (absent) cursor between two rows still pages correctly: Seek
	// lands on the next key, Prev steps below the gap.
	gap := fmt.Sprintf("%019d-zzz", 3) // sorts between rows[2] (ns 3) and rows[3] (ns 4)
	assert.Equal(t, []string{rows[2].ID, rows[1].ID, rows[0].ID},
		query(10, gap, FullProbeResultFilter{}))
	// A cursor newer than every row yields the whole history.
	assert.Len(t, query(10, fmt.Sprintf("%019d-zzz", 99), FullProbeResultFilter{}), 5)
	// Cursor and filter compose.
	assert.Equal(t, []string{rows[1].ID},
		query(10, rows[3].ID, FullProbeResultFilter{Outcome: "failed"}))

	// Unknown / evicted job: empty, not an error.
	got, err := s.FullProbeResults("fp-absent", 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.NotNil(t, got, "an empty page marshals as [], not null")
}

// TestFullProbeRowPerJobPrune drives the belt-and-suspenders per-job cap at
// the flush level: with the counters claiming a full job plus a global total
// that still fits (so eviction cannot mask the prune), one more row prunes
// exactly the oldest key (re-seeking First after each Delete) and marks the
// job's snapshot RowsPruned without touching its terminal state.
func TestFullProbeRowPerJobPrune(t *testing.T) {
	s, err := Open(t.TempDir() + "/jobprune.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-cap", State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
		TotalAttempts: 10,
	}))
	base := time.Now()
	const seed = 10
	for i := 0; i < seed; i++ {
		offerProbe(t, s, core.HTTPProbe{
			ID: fmt.Sprintf("%019d-r%d", i, i), Route: "relay", Target: "http://example.com",
			Success: true, StartedAt: base, FullProbeID: "fp-cap", Depth: 1,
		})
	}
	awaitJobRows(t, s, "fp-cap", seed)

	// Simulate a job sitting exactly at the per-job cap (writing 500000 real
	// rows would measure disk, not the prune). Keep the global total AT the
	// budget, not over it: the new row crosses the per-job cap only, so the
	// per-job prune — not wholesale eviction — must handle it.
	s.probeWriteMu.Lock()
	s.fpJobRows["fp-cap"] = fullProbeRowsPerJob
	s.fpRowsTotal.Store(fullProbeRowsTotal)
	s.probeWriteMu.Unlock()

	offerProbe(t, s, core.HTTPProbe{
		ID: fmt.Sprintf("%019d-new", seed), Route: "relay", Target: "http://example.com",
		Success: true, StartedAt: base, FullProbeID: "fp-cap", Depth: 1,
	})
	newKey := []byte(fmt.Sprintf("%019d-new", seed))
	require.Eventually(t, func() bool {
		var landed bool
		_ = s.db.View(func(tx *bolt.Tx) error {
			jb := tx.Bucket(bucketFullProbeRows).Bucket([]byte("fp-cap"))
			landed = jb != nil && jb.Get(newKey) != nil
			return nil
		})
		return landed && countJobRows(t, s, "fp-cap") == seed &&
			s.fpRowsTotal.Load() == fullProbeRowsTotal
	}, 10*time.Second, 10*time.Millisecond, "per-job prune never ran")

	// Exactly the oldest key was dropped; the newest row survives.
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		jb := tx.Bucket(bucketFullProbeRows).Bucket([]byte("fp-cap"))
		assert.Nil(t, jb.Get([]byte(fmt.Sprintf("%019d-r0", 0))), "oldest row must be pruned")
		assert.NotNil(t, jb.Get([]byte(fmt.Sprintf("%019d-new", seed))))
		return nil
	}))
	j, ok := s.FullProbeJob("fp-cap")
	require.True(t, ok)
	assert.True(t, j.RowsPruned, "pruned rows must mark the job snapshot")
	assert.Equal(t, core.FullProbeDone, j.State, "the mark must preserve the terminal state")
	assert.Equal(t, 10, j.TotalAttempts, "attempt counts stay exact")
	s.probeWriteMu.Lock()
	assert.Equal(t, int64(fullProbeRowsPerJob), s.fpJobRows["fp-cap"])
	s.probeWriteMu.Unlock()
	assert.Equal(t, int64(fullProbeRowsTotal), s.fpRowsTotal.Load())
}

// TestFullProbeRowEvictionSkipsRunning asserts the job-count budget evicts
// the oldest FINISHED job buckets wholesale, never a running job's — even
// when the running job is the oldest — and marks each evicted job's snapshot
// RowsPruned while preserving its terminal fields.
func TestFullProbeRowEvictionSkipsRunning(t *testing.T) {
	s, err := Open(t.TempDir() + "/evict.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	// The oldest job is RUNNING: its rows must survive every eviction pass.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: fpJobID(0), State: core.FullProbeRunning, StartedAt: start,
	}))
	offerProbe(t, s, core.HTTPProbe{
		ID: "running-row", Route: "relay", Target: "http://example.com",
		StartedAt: start, FullProbeID: fpJobID(0), Depth: 1,
	})
	awaitJobRows(t, s, fpJobID(0), 1)

	// Nine finished jobs (buckets 1..9): with the running one that is ten
	// buckets, over the 8-job budget. Each landing flush evicts the oldest
	// finished buckets until only running ones would remain.
	for i := 1; i <= fullProbeRowJobs+1; i++ {
		id := fpJobID(i)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start.Add(time.Duration(i) * time.Second),
			TotalAttempts: 3, Levels: []core.FullProbeLevel{{Depth: 1, Done: 3}},
		}))
		for k := 0; k < 3; k++ {
			offerProbe(t, s, core.HTTPProbe{
				ID: fmt.Sprintf("%019d-%s-r%d", k, id, k), Route: "relay", Target: "http://example.com",
				Success: k%2 == 0, StartedAt: start, FullProbeID: id, Depth: 1,
			})
		}
		awaitJobRows(t, s, id, 3)
	}

	// Steady state: the running bucket plus the newest finished ones, within
	// the job budget; the oldest finished buckets are gone wholesale.
	require.Eventually(t, func() bool {
		s.probeWriteMu.Lock()
		defer s.probeWriteMu.Unlock()
		return len(s.fpJobRows) <= fullProbeRowJobs
	}, 10*time.Second, 10*time.Millisecond, "eviction never brought the job count within budget")

	assert.Equal(t, 1, countJobRows(t, s, fpJobID(0)), "a running job's rows are never evicted")
	assert.Equal(t, 0, countJobRows(t, s, fpJobID(1)), "oldest finished bucket evicted wholesale")
	assert.Equal(t, 0, countJobRows(t, s, fpJobID(2)))
	assert.Equal(t, 3, countJobRows(t, s, fpJobID(fullProbeRowJobs+1)), "newest finished bucket survives")

	// Evicted jobs keep their terminal snapshot, marked RowsPruned.
	j, ok := s.FullProbeJob(fpJobID(1))
	require.True(t, ok, "eviction drops rows, not the job snapshot")
	assert.True(t, j.RowsPruned)
	assert.Equal(t, core.FullProbeDone, j.State)
	assert.Equal(t, 3, j.TotalAttempts)
	require.Len(t, j.Levels, 1)
	assert.Equal(t, 3, j.Levels[0].Done)
	// The running job is untouched and unmarked.
	rj, ok := s.FullProbeJob(fpJobID(0))
	require.True(t, ok)
	assert.Equal(t, core.FullProbeRunning, rj.State)
	assert.False(t, rj.RowsPruned)
	// A surviving finished job is unmarked.
	sj, ok := s.FullProbeJob(fpJobID(fullProbeRowJobs + 1))
	require.True(t, ok)
	assert.False(t, sj.RowsPruned)

	// Counters match reality: running 1 + the surviving finished buckets.
	s.probeWriteMu.Lock()
	var want int64
	for _, n := range s.fpJobRows {
		want += n
	}
	s.probeWriteMu.Unlock()
	assert.Equal(t, want, s.fpRowsTotal.Load())
	rows, err := s.FullProbeResults(fpJobID(1), 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	assert.Empty(t, rows, "an evicted job reads as an empty page")
}

// TestFullProbeRowGlobalBudgetEviction drives the global row budget directly
// (500000 real rows would measure disk, not the policy): with counters over
// budget, the next flush evicts oldest finished buckets until the total fits,
// skipping running jobs even when that leaves the budget exceeded.
func TestFullProbeRowGlobalBudgetEviction(t *testing.T) {
	s, err := Open(t.TempDir() + "/budget.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	// Three buckets: two finished claiming 500000 rows each and a newer
	// RUNNING one. Evicting the oldest finished bucket still leaves the
	// total over the 500000 budget, so the pass must continue to the second
	// finished bucket — and stop there, never touching the running one
	// (bounded transient overshoot).
	for i, state := range []string{core.FullProbeDone, core.FullProbeDone, core.FullProbeRunning} {
		id := fpJobID(i)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: id, State: state, StartedAt: start, FinishedAt: start,
		}))
		offerProbe(t, s, core.HTTPProbe{
			ID: fmt.Sprintf("row-%d", i), Route: "relay", Target: "http://example.com",
			StartedAt: start, FullProbeID: id, Depth: 1,
		})
		awaitJobRows(t, s, id, 1)
	}
	s.probeWriteMu.Lock()
	s.fpJobRows[fpJobID(0)] = 500000
	s.fpJobRows[fpJobID(1)] = 500000
	s.fpRowsTotal.Store(1000001)
	s.probeWriteMu.Unlock()

	// One more row triggers a flush whose eviction pass reads the inflated
	// baselines.
	offerProbe(t, s, core.HTTPProbe{
		ID: "row-trigger", Route: "relay", Target: "http://example.com",
		StartedAt: start, FullProbeID: fpJobID(2), Depth: 1,
	})
	require.Eventually(t, func() bool {
		return countJobRows(t, s, fpJobID(0)) == 0 && countJobRows(t, s, fpJobID(1)) == 0
	}, 10*time.Second, 10*time.Millisecond, "global budget never evicted both finished buckets")

	assert.Equal(t, 2, countJobRows(t, s, fpJobID(2)), "running bucket survives even over budget")
	s.probeWriteMu.Lock()
	assert.Equal(t, int64(2), s.fpJobRows[fpJobID(2)])
	_, gone0 := s.fpJobRows[fpJobID(0)]
	_, gone1 := s.fpJobRows[fpJobID(1)]
	assert.Len(t, s.fpJobRows, 1)
	s.probeWriteMu.Unlock()
	assert.False(t, gone0)
	assert.False(t, gone1)
	assert.Equal(t, int64(2), s.fpRowsTotal.Load(), "total drops by each evicted bucket's count")
	for i := 0; i < 2; i++ {
		j, ok := s.FullProbeJob(fpJobID(i))
		require.True(t, ok)
		assert.True(t, j.RowsPruned)
		assert.Equal(t, core.FullProbeDone, j.State)
	}
}

// TestRowsPrunedMarkSurvivesSnapshots asserts the retention mark is sticky:
// neither a later terminal snapshot nor a queued progress snapshot (which
// the runner builds without ever seeing RowsPruned) may clear it, and the
// terminal no-clobber rule still holds.
func TestRowsPrunedMarkSurvivesSnapshots(t *testing.T) {
	s, err := Open(t.TempDir() + "/sticky.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	start := time.Now()
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-sticky", State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
		RowsPruned: true,
	}))
	// A terminal re-landing without the mark preserves it.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-sticky", State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
		TotalAttempts: 7,
	}))
	j, ok := s.FullProbeJob("fp-sticky")
	require.True(t, ok)
	assert.True(t, j.RowsPruned, "terminal overwrite must preserve the retention mark")
	assert.Equal(t, 7, j.TotalAttempts)

	// A running job marked by retention keeps the mark through progress
	// snapshots, and a queued progress snapshot still cannot clobber a
	// terminal record.
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-prog", State: core.FullProbeRunning, StartedAt: start, RowsPruned: true,
	}))
	require.Eventually(t, func() bool {
		j, ok := s.FullProbeJob("fp-prog")
		return ok && j.RowsPruned
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-prog", State: core.FullProbeRunning, StartedAt: start,
		Levels: []core.FullProbeLevel{{Depth: 1, Done: 2}},
	}))
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-prog", State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
	}))
	require.Eventually(t, func() bool {
		j, ok := s.FullProbeJob("fp-prog")
		return ok && j.State == core.FullProbeDone
	}, 5*time.Second, 10*time.Millisecond)
	j, ok = s.FullProbeJob("fp-prog")
	require.True(t, ok)
	assert.True(t, j.RowsPruned, "progress/terminal snapshots must not clear the mark")
}

// TestOpenSeedsFullProbeRowBuckets asserts Open seeds the per-job and global
// counters keys-only from the nested buckets, serves the rows after a
// restart, and prunes an over-budget leftover DB once at open.
func TestOpenSeedsFullProbeRowBuckets(t *testing.T) {
	path := t.TempDir() + "/seed.db"
	s, err := Open(path)
	require.NoError(t, err)
	start := time.Now()
	for i := 0; i < 3; i++ {
		id := fpJobID(i)
		require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
			ID: id, State: core.FullProbeDone, StartedAt: start, FinishedAt: start,
		}))
		for k := 0; k < 2; k++ {
			offerProbe(t, s, core.HTTPProbe{
				ID: fmt.Sprintf("%019d-%s-r%d", k, id, k), Route: "relay", Target: "http://example.com",
				Success: true, StartedAt: start, FullProbeID: id, Depth: 1,
			})
		}
		awaitJobRows(t, s, id, 2)
	}
	require.NoError(t, s.Close())

	s2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	s2.probeWriteMu.Lock()
	assert.Len(t, s2.fpJobRows, 3)
	assert.Equal(t, int64(2), s2.fpJobRows[fpJobID(0)])
	s2.probeWriteMu.Unlock()
	assert.Equal(t, int64(6), s2.fpRowsTotal.Load())
	rows, err := s2.FullProbeResults(fpJobID(1), 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	assert.Len(t, rows, 2, "rows survive a restart")

	// Over-budget leftover: ten finished job buckets on disk. Open prunes to
	// the job budget once, before the writers start.
	require.NoError(t, s2.Close())
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		fr := tx.Bucket(bucketFullProbeRows)
		fb := tx.Bucket(bucketFullProbes)
		for i := 3; i < 13; i++ {
			id := fpJobID(i)
			jb, e := fr.CreateBucketIfNotExists([]byte(id))
			if e != nil {
				return e
			}
			if e := jb.Put([]byte(fmt.Sprintf("r%d", i)), []byte(`{"success":true}`)); e != nil {
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

	s3, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s3.Close() })
	s3.probeWriteMu.Lock()
	assert.Len(t, s3.fpJobRows, fullProbeRowJobs, "Open must prune an over-budget DB once")
	_, oldest := s3.fpJobRows[fpJobID(0)]
	s3.probeWriteMu.Unlock()
	assert.False(t, oldest, "oldest finished buckets evicted at open")
	j, ok := s3.FullProbeJob(fpJobID(0))
	require.True(t, ok)
	assert.True(t, j.RowsPruned)
	assert.Equal(t, int64(fullProbeRowJobs), s3.fpRowsTotal.Load())
}

// TestFullProbeRowCounterExactness hammers the job-bucket writer with
// duplicate keys across flush boundaries: Get-before-Put must count an
// overwrite as no row, and the per-job/global counters must equal the real
// key counts afterwards (Batch re-runs reload baselines per attempt).
func TestFullProbeRowCounterExactness(t *testing.T) {
	s, err := Open(t.TempDir() + "/exact.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	base := time.Now()
	const n = 150 // > probeBatchSize: several flushes, duplicates across them
	for i := 0; i < n; i++ {
		p := core.HTTPProbe{
			ID:    fmt.Sprintf("%019d-relay-http://example.com", i),
			Route: "relay", Target: "http://example.com", Success: true,
			StartedAt: base, FullProbeID: "fp-exact", Depth: 1,
		}
		offerProbe(t, s, p)
		offerProbe(t, s, p) // same key again: an overwrite, not a new row
	}
	awaitJobRows(t, s, "fp-exact", n)
	assert.Equal(t, n, countJobRows(t, s, "fp-exact"))
	assert.Equal(t, int64(n), s.fpRowsTotal.Load())
	assert.Equal(t, int64(n), s.probeRows.Load(), "success mirror counts once per key too")

	// A second job interleaved: counters stay per-job exact.
	for i := 0; i < 20; i++ {
		offerProbe(t, s, core.HTTPProbe{
			ID:    fmt.Sprintf("%019d-work-http://example.com", i),
			Route: "work", Target: "http://example.com",
			StartedAt: base, FullProbeID: "fp-exact2", Depth: 1,
		})
	}
	awaitJobRows(t, s, "fp-exact2", 20)
	s.probeWriteMu.Lock()
	assert.Equal(t, int64(n), s.fpJobRows["fp-exact"])
	assert.Equal(t, int64(20), s.fpJobRows["fp-exact2"])
	s.probeWriteMu.Unlock()
	assert.Equal(t, int64(n+20), s.fpRowsTotal.Load())
}

// TestCloseDrainsFullProbeRows asserts Close flushes pending job-bucket rows:
// rows enqueued immediately before Close are durable after it returns.
func TestCloseDrainsFullProbeRows(t *testing.T) {
	path := t.TempDir() + "/drain.db"
	s, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, s.RecordFullProbeJob(&core.FullProbeJob{
		ID: "fp-drain", State: core.FullProbeDone, StartedAt: time.Now(),
	}))
	require.NoError(t, s.RecordHTTPProbeAsync(core.HTTPProbe{
		ID: "row-drain", Route: "relay", Target: "http://example.com",
		StartedAt: time.Now(), FullProbeID: "fp-drain", Depth: 1, Reason: "connect_refused",
	}))
	require.NoError(t, s.Close())

	s2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() })
	rows, err := s2.FullProbeResults("fp-drain", 10, "", FullProbeResultFilter{})
	require.NoError(t, err)
	require.Len(t, rows, 1, "Close must flush pending job rows")
	assert.Equal(t, "row-drain", rows[0].ID)
	assert.Equal(t, int64(1), s2.fpRowsTotal.Load())
}
