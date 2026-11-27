package metrics

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
