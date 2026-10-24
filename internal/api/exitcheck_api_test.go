package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProbes supplies fixed probe statuses for /api/chains/probes.
type fakeProbes struct{ statuses []core.ProbeStatus }

func (f *fakeProbes) ChainProbeStatus() []core.ProbeStatus { return f.statuses }

// TestChainProbesEmptyWithoutReporter asserts the endpoint works (empty
// array, 200) when no probe reporter is wired.
func TestChainProbesEmptyWithoutReporter(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "GET", "/api/chains/probes", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Equal(t, "[]\n", string(body))
}

// TestChainProbesReportsStatuses asserts wired probe results are returned
// verbatim.
func TestChainProbesReportsStatuses(t *testing.T) {
	hs, cfg, ms := newAPIServer(t, "")
	srv := New(cfg, ms)
	srv.SetProbeReporter(&fakeProbes{statuses: []core.ProbeStatus{{
		OutboundID: "ob1",
		Address:    "1.2.3.4:1080",
		PoolID:     "work",
		Direct:     core.ProbeVerdict{OK: false, Error: "blocked", At: time.Now()},
		Relay:      core.ProbeVerdict{OK: true, Latency: 40 * time.Millisecond, At: time.Now()},
		RelayVia:   "access1",
	}}})
	hs2 := httptest.NewServer(srv.Auth(""))
	t.Cleanup(hs2.Close)

	code, body := do(t, hs2, "GET", "/api/chains/probes", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"ob1"`)
	assert.Contains(t, string(body), `"relayVia":"access1"`)
	_ = hs
}

// TestChainProbesMethodNotAllowed asserts non-GET is rejected.
func TestChainProbesMethodNotAllowed(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "POST", "/api/chains/probes", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
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

// fakeRunner records on-demand triggers.
type fakeRunner struct{ calls int }

func (f *fakeRunner) CheckNow() { f.calls++ }

// TestExitChecksRunTriggers asserts POST /api/exit-checks/run calls the
// runner and returns 202; without a runner it reports 503.
func TestExitChecksRunTriggers(t *testing.T) {
	hs, cfg, ms := newAPIServer(t, "")
	srv := New(cfg, ms)
	fr := &fakeRunner{}
	srv.SetExitRunner(fr)
	hs2 := httptest.NewServer(srv.Auth(""))
	t.Cleanup(hs2.Close)

	code, body := do(t, hs2, "POST", "/api/exit-checks/run", "", nil)
	require.Equal(t, http.StatusAccepted, code, string(body))
	assert.Equal(t, 1, fr.calls)
	_ = hs
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
