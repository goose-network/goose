package router

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// awaitFullProbe polls a job until it leaves the running state (or fails the
// test), returning the terminal snapshot.
func awaitFullProbe(t *testing.T, r *Router, id string) *core.FullProbeJob {
	t.Helper()
	var out *core.FullProbeJob
	require.Eventually(t, func() bool {
		j, ok := r.FullProbeJob(id)
		if !ok {
			return false
		}
		if j.State == core.FullProbeRunning {
			return false
		}
		out = j
		return true
	}, 20*time.Second, 25*time.Millisecond, "full probe job %s never finished", id)
	return out
}

// hopsOf joins a path's hops for readable set membership assertions.
func hopsOf(p core.FullProbePath) string {
	s := ""
	for i, h := range p.Hops {
		if i > 0 {
			s += ">"
		}
		s += h
	}
	return s
}

// TestFullProbeDiscoversRescuePath is the core workflow: the local network
// blocks the work proxy, so level 1 finds relay working (A) and work failed
// (B); level 2 relays work through relay and finds local→relay→work working
// (C). The successful paths are exactly what inbound creation builds from.
// It uses the real SOCKS5 plugins and the blocking stack, so the rescue is a
// real end-to-end proxied HTTP request, not a fake handshake.
func TestFullProbeDiscoversRescuePath(t *testing.T) {
	f := newReachFixture(t, true) // local network blocks the work server
	srv := probeTarget(t)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target:   srv.URL,
		Timeout:  5 * time.Second,
		RPS:      1000,
		MaxDepth: 5,
	})
	require.NoError(t, err)
	require.Equal(t, core.FullProbeRunning, j0.State)

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.GreaterOrEqual(t, len(j.Levels), 2, "the rescue needs at least two levels")

	// Level 1: relay succeeds, work fails.
	l1 := j.Levels[0]
	assert.Equal(t, 1, l1.Depth)
	assert.Equal(t, 2, l1.Total)
	assert.Equal(t, 1, l1.Success)
	assert.Equal(t, 1, l1.Failed)
	var l1ok, l1fail []string
	for _, p := range l1.SuccessPaths {
		l1ok = append(l1ok, hopsOf(p))
	}
	for _, fi := range l1.FailedItems {
		l1fail = append(l1fail, fi.ID)
	}
	assert.Equal(t, []string{"relay"}, l1ok)
	assert.Equal(t, []string{"work"}, l1fail)

	// Level 2: local→relay→work succeeds.
	l2 := j.Levels[1]
	assert.Equal(t, 2, l2.Depth)
	assert.Equal(t, 1, l2.Prefixes)
	assert.Equal(t, 1, l2.Candidates)
	require.Len(t, l2.SuccessPaths, 1)
	assert.Equal(t, []string{"relay", "work"}, l2.SuccessPaths[0].Hops)
	assert.Equal(t, http.StatusOK, l2.SuccessPaths[0].Status)
	assert.Greater(t, l2.SuccessPaths[0].Latency, time.Duration(0))

	// BestPaths: the fastest success per depth, depth ascending.
	best := j.BestPaths()
	require.Len(t, best, 2)
	assert.Equal(t, []string{"relay"}, best[0].Hops)
	assert.Equal(t, []string{"relay", "work"}, best[1].Hops)

	// A success probe row is persisted asynchronously, linked to the job.
	require.Eventually(t, func() bool {
		rows, err := f.ms.RecentHTTPProbes(50, "")
		require.NoError(t, err)
		for _, p := range rows {
			if p.FullProbeID == j.ID && p.Depth == 2 && p.Success {
				return true
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond, "depth-2 success probe row never landed")

	// The success also lands as an informational metric row (chain starts
	// with the probe pseudo-hop, so it never feeds reachability verdicts).
	require.Eventually(t, func() bool {
		rows, err := f.ms.Recent(200)
		require.NoError(t, err)
		for _, m := range rows {
			if len(m.Chain) == 3 && m.Chain[0] == core.HopProbe && m.Chain[1] == "relay" && m.Chain[2] == "work" && m.Success {
				return true
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond, "rescue metric row never landed")
}

// TestFullProbeMaxDepthOne verifies a depth-1 job probes only local→outbound
// and never attempts the rescue, so the blocked work server stays failed.
func TestFullProbeMaxDepthOne(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target:   srv.URL,
		Timeout:  5 * time.Second,
		RPS:      1000,
		MaxDepth: 1,
	})
	require.NoError(t, err)
	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State)
	require.Len(t, j.Levels, 1)
	assert.Equal(t, 1, j.Levels[0].Success)
	assert.Equal(t, 1, j.Levels[0].Failed)
	best := j.BestPaths()
	require.Len(t, best, 1)
	assert.Equal(t, []string{"relay"}, best[0].Hops)
}

// TestFullProbeFanoutCap verifies MaxAttemptsPerLevel bounds one level's
// fan-out and counts the cut pairs as Skipped, not failed.
func TestFullProbeFanoutCap(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target:              srv.URL,
		Timeout:             5 * time.Second,
		RPS:                 1000,
		MaxDepth:            1,
		MaxAttemptsPerLevel: 1,
	})
	require.NoError(t, err)
	j := awaitFullProbe(t, f.router, j0.ID)
	require.Len(t, j.Levels, 1)
	assert.Equal(t, 1, j.Levels[0].Total)
	assert.Equal(t, 1, j.Levels[0].Skipped)
	assert.Equal(t, 1, j.Levels[0].Done)
}

// TestFullProbeCancel verifies a canceled job stops issuance and lands in the
// canceled state with the levels it completed. A slow target keeps attempts
// in flight long enough to cancel mid-sweep.
func TestFullProbeCancel(t *testing.T) {
	f := newReachFixture(t, false)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target:      slow.URL,
		Timeout:     2 * time.Second,
		RPS:         1,
		Concurrency: 2,
		MaxDepth:    5,
	})
	require.NoError(t, err)
	require.True(t, f.router.CancelFullProbe(j0.ID))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeCanceled, j.State)
	// A second cancel finds no running job.
	assert.False(t, f.router.CancelFullProbe(j0.ID))
}

// TestFullProbeStartValidation verifies the synchronous rejections: a bad
// target and an engine with no outbounds.
func TestFullProbeStartValidation(t *testing.T) {
	f := newReachFixture(t, false)
	_, err := f.router.StartFullProbe(core.FullProbeSpec{Target: "not-a-url"})
	require.Error(t, err)

	empty, _ := testRouter(t, nil, nil, nil, nil)
	t.Cleanup(empty.Close)
	_, err = empty.StartFullProbe(core.FullProbeSpec{Target: "http://example.com"})
	require.Error(t, err, "no outbounds to probe")
}

// TestFullProbeRunningJobLimit verifies at most FullProbeMaxRunningJobs jobs
// run at once; a further start is rejected until one finishes.
func TestFullProbeRunningJobLimit(t *testing.T) {
	f := newReachFixture(t, false)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)
	spec := core.FullProbeSpec{Target: slow.URL, Timeout: 2 * time.Second, RPS: 1, Concurrency: 1, MaxDepth: 1}

	var ids []string
	for i := 0; i < core.FullProbeMaxRunningJobs; i++ {
		j, err := f.router.StartFullProbe(spec)
		require.NoError(t, err)
		ids = append(ids, j.ID)
	}
	_, err := f.router.StartFullProbe(spec)
	require.Error(t, err, "running-job limit should reject a further start")
	for _, id := range ids {
		f.router.CancelFullProbe(id)
		awaitFullProbe(t, f.router, id)
	}
}

// TestFullProbeEvictsFinishedJobs asserts finished jobs leave the live map
// (their durable snapshot serves GET/list from then on): otherwise every
// sweep's full result set accumulates in memory for the engine's lifetime.
func TestFullProbeEvictsFinishedJobs(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 2,
	})
	require.NoError(t, err)
	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State)

	// The runner evicts before closing done; wait for the eviction.
	require.Eventually(t, func() bool {
		f.router.fullProbeMu.Lock()
		defer f.router.fullProbeMu.Unlock()
		_, ok := f.router.fullProbes[j0.ID]
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "finished job never evicted from the live map")

	// GET still serves the terminal state from durable history, and the
	// list merges it without a live copy.
	got, ok := f.router.FullProbeJob(j0.ID)
	require.True(t, ok)
	assert.Equal(t, core.FullProbeDone, got.State)
	assert.Len(t, got.BestPaths(), 2)
	list := f.router.FullProbeJobs(10)
	require.NotEmpty(t, list)
	assert.Equal(t, j0.ID, list[0].ID)
	// A finished job cannot be canceled.
	assert.False(t, f.router.CancelFullProbe(j0.ID))
}

// TestFullProbeCapacitySentinel asserts the running-job-limit rejection
// wraps core.ErrFullProbeCapacity so the API can answer 409 (a transient
// conflict) rather than 400 (a bad spec).
func TestFullProbeCapacitySentinel(t *testing.T) {
	f := newReachFixture(t, false)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)
	spec := core.FullProbeSpec{Target: slow.URL, Timeout: 2 * time.Second, RPS: 1, Concurrency: 1, MaxDepth: 1}

	var ids []string
	for i := 0; i < core.FullProbeMaxRunningJobs; i++ {
		j, err := f.router.StartFullProbe(spec)
		require.NoError(t, err)
		ids = append(ids, j.ID)
	}
	_, err := f.router.StartFullProbe(spec)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrFullProbeCapacity), "capacity rejection must wrap the sentinel, got: %v", err)
	for _, id := range ids {
		f.router.CancelFullProbe(id)
		awaitFullProbe(t, f.router, id)
	}
}
