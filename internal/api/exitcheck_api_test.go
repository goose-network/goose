package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/exitip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricsListsProbeRows asserts the metrics surface carries reachability
// probe rows (chain ["local", outbound]) alongside request rows — the probes
// endpoint is gone because the metrics table is the single view.
func TestMetricsListsProbeRows(t *testing.T) {
	hs, _, ms := newAPIServer(t, "")
	require.NoError(t, ms.Record(core.RequestMetric{
		ID:        "p1",
		Chain:     []string{core.HopLocal, "ob1"},
		Target:    "1.2.3.4:1080",
		Success:   false,
		Error:     "blocked",
		StartedAt: time.Now(),
	}))
	// DB writes are batched asynchronously; poll until the row surfaces.
	require.Eventually(t, func() bool {
		code, body := do(t, hs, "GET", "/api/metrics", "", nil)
		return code == http.StatusOK &&
			strings.Contains(string(body), `"local"`) &&
			strings.Contains(string(body), `"ob1"`)
	}, 5*time.Second, 100*time.Millisecond)
}

// TestChainProbesRouteGone asserts the retired probes endpoint no longer
// resolves to a probes view: the path falls into the chains subtree and
// reports an unknown chain.
func TestChainProbesRouteGone(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "GET", "/api/chains/probes", "", nil)
	assert.Equal(t, http.StatusNotFound, code, string(body))
}

// TestExitChecksListAndFilter asserts recorded checks are listed newest
// first and ?outbound_id= filters them.
func TestExitChecksListAndFilter(t *testing.T) {
	hs, _, ms := newAPIServer(t, "")
	base := time.Now().Add(-time.Hour)
	require.NoError(t, ms.RecordExitCheck(core.ExitCheck{
		ID: "c1", OutboundID: "a", Site: "ifconfig.me", ExitIP: "1.2.3.4", Success: true, StartedAt: base,
	}))
	require.NoError(t, ms.RecordExitCheck(core.ExitCheck{
		ID: "c2", OutboundID: "b", Site: "ipinfo.io", ExitIP: "5.6.7.8", Success: true, StartedAt: base.Add(time.Minute),
	}))

	code, body := do(t, hs, "GET", "/api/exit-checks", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"c2"`)
	assert.Contains(t, string(body), `"c1"`)

	code, body = do(t, hs, "GET", "/api/exit-checks?outbound_id=a&n=1", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"c1"`)
	assert.NotContains(t, string(body), `"c2"`)
}

// TestExitChecksMethodNotAllowed asserts non-GET on the collection is 405.
func TestExitChecksMethodNotAllowed(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "POST", "/api/exit-checks", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
}

// fakeRunner records on-demand triggers and returns canned results. When
// block is non-nil the round "runs" until that channel is closed, which is
// how the accepted-while-running path is tested without a real wait.
type fakeRunner struct {
	mu    sync.Mutex
	calls int
	specs []*exitip.RunSpec
	res   exitip.RunResult
	block chan struct{}
}

func (f *fakeRunner) CheckNow(spec *exitip.RunSpec) <-chan exitip.RunResult {
	f.mu.Lock()
	f.calls++
	f.specs = append(f.specs, spec)
	res, block := f.res, f.block
	f.mu.Unlock()
	out := make(chan exitip.RunResult, 1)
	go func() {
		if block != nil {
			<-block
		}
		out <- res
	}()
	return out
}

func (f *fakeRunner) lastSpec() *exitip.RunSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.specs[len(f.specs)-1]
}

// newRunServer builds a server whose exit runner is a fakeRunner.
func newRunServer(t *testing.T, fr *fakeRunner) *httptest.Server {
	t.Helper()
	_, cfg, ms := newAPIServer(t, "")
	srv := New(cfg, ms)
	srv.SetExitRunner(fr)
	hs := httptest.NewServer(srv.Auth(""))
	t.Cleanup(hs.Close)
	return hs
}

// TestExitChecksRunReturnsResults asserts a run that finishes inside the wait
// window answers 200 with the round's records — a single-run task gets its
// results in the response instead of polling history.
func TestExitChecksRunReturnsResults(t *testing.T) {
	fr := &fakeRunner{res: exitip.RunResult{Checks: []core.ExitCheck{{
		ID: "c1", OutboundID: "a", Site: "ifconfig.me", ExitIP: "1.2.3.4", Success: true,
	}}}}
	hs := newRunServer(t, fr)

	code, body := do(t, hs, "POST", "/api/exit-checks/run", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"status":"complete"`)
	assert.Contains(t, string(body), `"1.2.3.4"`)
	assert.Equal(t, 1, fr.calls)
}

// TestExitChecksRunPassesSpec asserts an optional body reaches the runner as
// run overrides, and an absent body reaches it as nil (use the config).
func TestExitChecksRunPassesSpec(t *testing.T) {
	fr := &fakeRunner{}
	hs := newRunServer(t, fr)

	code, body := do(t, hs, "POST", "/api/exit-checks/run", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Nil(t, fr.lastSpec(), "an empty body means no overrides")

	code, body = do(t, hs, "POST", "/api/exit-checks/run", "", map[string]any{
		"sites":           []string{"https://ifconfig.me"},
		"outbounds":       []string{"a", "b"},
		"timeout_seconds": 7,
		"concurrency":     2,
	})
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Equal(t, &exitip.RunSpec{
		Sites:       []string{"https://ifconfig.me"},
		Outbounds:   []string{"a", "b"},
		TimeoutSec:  7,
		Concurrency: 2,
	}, fr.lastSpec())
}

// TestExitChecksRunRejectsUnknownField asserts the body is decoded strictly,
// so a typo in an override is a 400 rather than a silently ignored field.
func TestExitChecksRunRejectsUnknownField(t *testing.T) {
	hs := newRunServer(t, &fakeRunner{})
	code, body := do(t, hs, "POST", "/api/exit-checks/run", "", map[string]any{"site": "https://ifconfig.me"})
	require.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "site")
}

// TestExitChecksRunAcceptsSlowRound asserts a round still running when the
// wait window expires answers 202 and keeps going in the background.
func TestExitChecksRunAcceptsSlowRound(t *testing.T) {
	restore := exitRunWait
	exitRunWait = 50 * time.Millisecond
	t.Cleanup(func() { exitRunWait = restore })

	release := make(chan struct{})
	fr := &fakeRunner{block: release}
	hs := newRunServer(t, fr)
	t.Cleanup(func() { close(release) })

	code, body := do(t, hs, "POST", "/api/exit-checks/run", "", nil)
	require.Equal(t, http.StatusAccepted, code, string(body))
	assert.Contains(t, string(body), `"status":"accepted"`)
	assert.Equal(t, 1, fr.calls, "the round was started even though the response did not wait for it")
}

func TestExitChecksRunWithoutRunner(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "POST", "/api/exit-checks/run", "", nil)
	assert.Equal(t, http.StatusServiceUnavailable, code)
}

// TestEngineRejectsBadCron asserts an invalid exit-check cron expression is
// rejected with 400 before persisting.
func TestEngineRejectsBadCron(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "PUT", "/api/engine", "", map[string]any{
		"exit_checks": map[string]any{"enabled": true, "cron": "61 * * * *"},
	})
	require.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "cron")
}

// TestEngineAcceptsValidCron asserts a valid cron persists and round-trips.
func TestEngineAcceptsValidCron(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "PUT", "/api/engine", "", map[string]any{
		"exit_checks": map[string]any{"enabled": true, "cron": "*/15 * * * *", "interval_seconds": 300},
	})
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"cron":"*/15 * * * *"`)
}
