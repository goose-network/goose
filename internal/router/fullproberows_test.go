package router

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// awaitJobResults polls the durable per-job attempt rows until want rows are
// readable, returning them.
func awaitJobResults(t *testing.T, f *reachFixture, jobID string, want int) []core.HTTPProbe {
	t.Helper()
	var out []core.HTTPProbe
	require.Eventually(t, func() bool {
		rows, err := f.ms.FullProbeResults(jobID, want+10, "", metrics.FullProbeResultFilter{})
		require.NoError(t, err)
		if len(rows) < want {
			return false
		}
		out = rows
		return true
	}, 10*time.Second, 25*time.Millisecond, "job %s attempt rows never landed", jobID)
	return out
}

// TestFullProbeFailureRowPersisted asserts a failed attempt persists a full
// evidence row in the job's result bucket (Error/Phase/Reason, timings, the
// partial Path, Depth and FullProbeID) while the shared probe history and the
// metric rows stay success-only.
func TestFullProbeFailureRowPersisted(t *testing.T) {
	f := newReachFixture(t, true) // local network blocks the work server
	srv := probeTarget(t)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
	})
	require.NoError(t, err)
	j := awaitFullProbe(t, f.router, j0.ID)
	require.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.Len(t, j.Levels, 1)
	require.Equal(t, 1, j.Levels[0].Success)
	require.Equal(t, 1, j.Levels[0].Failed)

	// Both attempts land in the job bucket: the blocked work failure and the
	// relay success.
	rows := awaitJobResults(t, f, j.ID, 2)
	byRoute := map[string]core.HTTPProbe{}
	for _, p := range rows {
		byRoute[p.Route] = p
		assert.Equal(t, j.ID, p.FullProbeID)
		assert.Equal(t, 1, p.Depth)
		assert.Equal(t, srv.URL, p.Target)
	}
	fail, ok := byRoute["work"]
	require.True(t, ok, "failure row for the blocked work hop missing")
	assert.False(t, fail.Success)
	assert.NotEmpty(t, fail.Error)
	assert.NotEmpty(t, fail.Phase, "failure row must locate the failing phase")
	assert.NotEmpty(t, fail.Reason, "failure row must carry the classified reason")
	assert.Greater(t, fail.Latency, time.Duration(0))
	require.NotNil(t, fail.Path, "failure row must carry the partial observed path")
	win, ok := byRoute["relay"]
	require.True(t, ok)
	assert.True(t, win.Success)
	assert.Equal(t, http.StatusOK, win.Status)

	// The failure filters server-side like any other row.
	filtered, err := f.ms.FullProbeResults(j.ID, 10, "", metrics.FullProbeResultFilter{Outcome: "failed", Hop: "work"})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, fail.ID, filtered[0].ID)

	// Shared probe history: the success mirror only, never the failure.
	require.Eventually(t, func() bool {
		shared, err := f.ms.RecentHTTPProbes(50, "")
		require.NoError(t, err)
		for _, p := range shared {
			if p.FullProbeID == j.ID && p.Route == "relay" && p.Success {
				return true
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond, "success mirror never landed in the shared history")
	shared, err := f.ms.RecentHTTPProbes(50, "")
	require.NoError(t, err)
	for _, p := range shared {
		if p.FullProbeID == j.ID {
			assert.True(t, p.Success, "failures must not enter the shared probe history")
		}
	}

	// Metric rows: the success lands (chain starts with the probe pseudo-hop);
	// the failure records NONE — a dead candidate must not flood the requests
	// bucket.
	require.Eventually(t, func() bool {
		mrows, err := f.ms.Recent(200)
		require.NoError(t, err)
		for _, m := range mrows {
			if len(m.Chain) == 2 && m.Chain[0] == core.HopProbe && m.Chain[1] == "relay" && m.Success {
				return true
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond, "success metric row never landed")
	mrows, err := f.ms.Recent(200)
	require.NoError(t, err)
	for _, m := range mrows {
		if len(m.Chain) == 2 && m.Chain[0] == core.HopProbe && m.Chain[1] == "work" {
			t.Errorf("failure metric row recorded for the blocked work hop: %+v", m)
		}
	}
}

// TestFullProbeCancelBeforeIssueNoRow asserts an attempt a cancel cut before
// issue leaves no row (it is Skipped, not a dead route in history), while an
// attempt already in flight genuinely ran and lands with reason "canceled".
func TestFullProbeCancelBeforeIssueNoRow(t *testing.T) {
	f := newReachFixture(t, false)
	var once sync.Once
	issued := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(issued) })
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)

	// One worker at 1 rps: the first attempt issues immediately, the second
	// waits a full second for its rate-limiter slot — the cancel cuts it
	// before issue.
	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: slow.URL, Timeout: 5 * time.Second, RPS: 1, Concurrency: 1, MaxDepth: 1,
	})
	require.NoError(t, err)
	select {
	case <-issued:
	case <-time.After(10 * time.Second):
		t.Fatal("first attempt never issued")
	}
	require.True(t, f.router.CancelFullProbe(j0.ID))
	j := awaitFullProbe(t, f.router, j0.ID)
	require.Equal(t, core.FullProbeCanceled, j.State)
	require.Len(t, j.Levels, 1)
	assert.Equal(t, 1, j.Levels[0].Skipped, "the unissued attempt must count as Skipped")

	// Exactly the in-flight attempt's row exists — canceled, not a fabricated
	// failure for the attempt that never left.
	rows := awaitJobResults(t, f, j.ID, 1)
	require.Len(t, rows, 1)
	assert.False(t, rows[0].Success)
	assert.Equal(t, "canceled", rows[0].Reason)
	assert.Equal(t, j.ID, rows[0].FullProbeID)
	// Give any (wrongly recorded) second row time to land, then re-check.
	time.Sleep(700 * time.Millisecond)
	rows, err = f.ms.FullProbeResults(j.ID, 10, "", metrics.FullProbeResultFilter{})
	require.NoError(t, err)
	assert.Len(t, rows, 1, "a cancel-cut attempt must leave no row")
}
