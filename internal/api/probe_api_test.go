package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProber stands in for the router: it records the spec it was handed and
// returns canned results, so these tests cover the HTTP surface (decoding,
// validation, status codes) without dialing anything. When block is non-nil the
// round "runs" until that channel is closed, which is how the
// accepted-while-running path is tested without a real wait. The handler runs
// the round in a goroutine, so the fields are mutex-guarded.
type fakeProber struct {
	mu    sync.Mutex
	specs []core.HTTPProbeSpec
	res   []core.HTTPProbe
	err   error
	block chan struct{}
	// complete, when non-nil, receives the round's return error once it finishes
	// — how a test observes a detached round outliving the request.
	complete chan error
}

func (f *fakeProber) RunHTTPProbes(ctx context.Context, spec core.HTTPProbeSpec) ([]core.HTTPProbe, error) {
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	res, err, block, complete := f.res, f.err, f.block, f.complete
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if err != nil {
		if complete != nil {
			complete <- err
		}
		return nil, err
	}
	if complete != nil {
		complete <- nil
	}
	return res, nil
}

func (f *fakeProber) snapshot() []core.HTTPProbeSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.HTTPProbeSpec(nil), f.specs...)
}

func newProbeServer(t *testing.T, fp *fakeProber) (*httptest.Server, *metrics.Store) {
	t.Helper()
	_, cfg, ms := newAPIServer(t, "")
	srv := New(cfg, ms)
	srv.SetProbeRunner(fp)
	hs := httptest.NewServer(srv.Auth(""))
	t.Cleanup(hs.Close)
	return hs, ms
}

// TestProbesRunReturnsResults asserts POST /api/probes/run hands the body to
// the runner as a spec and answers with the round's records.
func TestProbesRunReturnsResults(t *testing.T) {
	fp := &fakeProber{res: []core.HTTPProbe{{
		ID: "p1", Route: "outbound:a", Target: "https://example.com/ip", Method: "GET",
		Status: 200, Body: `{"ip":"1.2.3.4"}`, Success: true, Latency: 12 * time.Millisecond,
	}}}
	hs, _ := newProbeServer(t, fp)

	code, body := do(t, hs, "POST", "/api/probes/run", "", map[string]any{
		"targets":         []string{"https://example.com/ip"},
		"method":          "POST",
		"headers":         map[string]string{"X-Token": "t"},
		"body":            `{"a":1}`,
		"routes":          []string{"pool:default", "direct"},
		"timeout_seconds": 7,
		"max_body_bytes":  2048,
	})
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"outbound:a"`)
	// The probe body is a JSON string inside the response, so its quotes are
	// escaped: match the value, not the quoting.
	assert.Contains(t, string(body), `1.2.3.4`)

	specs := fp.snapshot()
	require.Len(t, specs, 1)
	spec := specs[0]
	assert.Equal(t, []string{"https://example.com/ip"}, spec.Targets)
	assert.Equal(t, "POST", spec.Method)
	assert.Equal(t, map[string]string{"X-Token": "t"}, spec.Headers)
	assert.Equal(t, `{"a":1}`, spec.Body)
	assert.Equal(t, []string{"pool:default", "direct"}, spec.Routes)
	assert.Equal(t, 7*time.Second, spec.Timeout)
	assert.Equal(t, 2048, spec.MaxBodyBytes)
}

// TestProbesRunRequiresTargets asserts a run with no targets is a 400, whether
// the body is absent or the list is empty: there is nothing to probe.
func TestProbesRunRequiresTargets(t *testing.T) {
	hs, _ := newProbeServer(t, &fakeProber{})

	code, body := do(t, hs, "POST", "/api/probes/run", "", nil)
	assert.Equal(t, http.StatusBadRequest, code, string(body))

	code, body = do(t, hs, "POST", "/api/probes/run", "", map[string]any{"targets": []string{}})
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "targets")
}

// TestProbesRunReportsUnrunnableSpec asserts a spec the runner refuses (a
// non-URL target, an over-large fan-out) is the caller's mistake: 400 with the
// reason, not a 500.
func TestProbesRunReportsUnrunnableSpec(t *testing.T) {
	fp := &fakeProber{err: context.DeadlineExceeded}
	hs, _ := newProbeServer(t, fp)

	code, body := do(t, hs, "POST", "/api/probes/run", "", map[string]any{"targets": []string{"https://example.com"}})
	require.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "deadline exceeded")
}

// TestProbesRunAcceptsSlowRound asserts a round that outlasts the wait window
// answers 202 with an accepted body while it keeps running, instead of holding
// the request open until a front proxy's read timeout cuts it off (the 504).
func TestProbesRunAcceptsSlowRound(t *testing.T) {
	restore := probeRunWait
	probeRunWait = 50 * time.Millisecond
	t.Cleanup(func() { probeRunWait = restore })

	release := make(chan struct{})
	fp := &fakeProber{block: release}
	hs, _ := newProbeServer(t, fp)
	t.Cleanup(func() { close(release) })

	code, body := do(t, hs, "POST", "/api/probes/run", "", map[string]any{"targets": []string{"https://example.com"}})
	require.Equal(t, http.StatusAccepted, code, string(body))
	assert.Contains(t, string(body), `"status":"accepted"`)
	assert.Len(t, fp.snapshot(), 1, "the round started even though the response did not wait for it")
}

// TestProbesRunRoundOutlivesRequest asserts the round runs detached from the
// request: once the handler has answered 202 and the HTTP request is over, the
// round still completes rather than being canceled by the request's context.
func TestProbesRunRoundOutlivesRequest(t *testing.T) {
	restore := probeRunWait
	probeRunWait = 50 * time.Millisecond
	t.Cleanup(func() { probeRunWait = restore })

	release := make(chan struct{})
	fp := &fakeProber{block: release, complete: make(chan error, 1)}
	hs, _ := newProbeServer(t, fp)

	code, body := do(t, hs, "POST", "/api/probes/run", "", map[string]any{"targets": []string{"https://example.com"}})
	require.Equal(t, http.StatusAccepted, code, string(body))

	// The request is over. A round tied to r.Context() would be canceled now; a
	// detached one finishes when its work does.
	close(release)
	select {
	case err := <-fp.complete:
		assert.NoError(t, err, "the detached round completes, not canceled by the request ending")
	case <-time.After(2 * time.Second):
		t.Fatal("the detached round never completed after the request ended")
	}
}

// TestProbesRunWithoutRunner asserts the endpoint reports 503 when the engine
// never wired a runner, rather than answering with empty results.
func TestProbesRunWithoutRunner(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "POST", "/api/probes/run", "", map[string]any{"targets": []string{"https://example.com"}})
	assert.Equal(t, http.StatusServiceUnavailable, code, string(body))
}

// TestProbesRunMethodNotAllowed asserts the trigger is POST-only and the
// results view is GET-only.
func TestProbesMethodNotAllowed(t *testing.T) {
	hs, _ := newProbeServer(t, &fakeProber{})
	code, _ := do(t, hs, "GET", "/api/probes/run", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
	code, _ = do(t, hs, "POST", "/api/probes/results", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
}

// TestProbeResultsListAndFilter asserts recorded probes are listed newest
// first and ?route= / ?n= filter them.
func TestProbeResultsListAndFilter(t *testing.T) {
	hs, ms := newProbeServer(t, &fakeProber{})
	base := time.Now().Add(-time.Hour)
	require.NoError(t, ms.RecordHTTPProbe(core.HTTPProbe{
		ID: "p1", Route: "outbound:a", Target: "https://example.com", Status: 200, StartedAt: base,
	}))
	require.NoError(t, ms.RecordHTTPProbe(core.HTTPProbe{
		ID: "p2", Route: "pool:default", Target: "https://example.com", Status: 502, StartedAt: base.Add(time.Minute),
	}))

	code, body := do(t, hs, "GET", "/api/probes/results", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Less(t, strings.Index(string(body), `"p2"`), strings.Index(string(body), `"p1"`), "newest first")

	code, body = do(t, hs, "GET", "/api/probes/results?route=outbound:a&n=5", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"p1"`)
	assert.NotContains(t, string(body), `"p2"`)
}

// TestProbeRowsDoNotSetReachability asserts an HTTP probe's metric row is
// informational: a failing probe through a route must not mark that route's
// outbound unreachable, because the failure may be the target's.
func TestProbeRowsDoNotSetReachability(t *testing.T) {
	hs, ms := newProbeServer(t, &fakeProber{})
	require.NoError(t, ms.Record(core.RequestMetric{
		ID:        "m1",
		Chain:     []string{core.HopProbe, "outbound:a"},
		Target:    "example.com:443",
		Success:   false,
		Error:     "dial refused",
		StartedAt: time.Now(),
	}))

	require.Eventually(t, func() bool {
		code, body := do(t, hs, "GET", "/api/metrics", "", nil)
		return code == http.StatusOK && strings.Contains(string(body), `"probe"`)
	}, 5*time.Second, 100*time.Millisecond)

	_, _, measured := ms.Reachability("outbound:a")
	assert.False(t, measured, "a probe row must not produce a reachability verdict")
	assert.Zero(t, ms.Stats("outbound:a").SuccessRate)
}

// TestEngineValidatesProbeTargets asserts a scheduled probe config with a
// non-URL target is rejected at PUT time, and a valid one round-trips.
func TestEngineValidatesProbeTargets(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")

	code, body := do(t, hs, "PUT", "/api/engine", "", map[string]any{
		"probes": map[string]any{"enabled": true, "http": map[string]any{
			"targets": []string{"example.com/ip"},
		}},
	})
	require.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "absolute http(s) URL")

	code, body = do(t, hs, "PUT", "/api/engine", "", map[string]any{
		"probes": map[string]any{"enabled": true, "interval_seconds": 60, "http": map[string]any{
			"targets": []string{"https://example.com/ip"},
			"method":  "HEAD",
			"routes":  []string{"pool:default"},
		}},
	})
	require.Equal(t, http.StatusOK, code, string(body))
	assert.Contains(t, string(body), `"https://example.com/ip"`)
	assert.Contains(t, string(body), `"HEAD"`)

	// An HTTP probe config with no targets is ignored, not invalid: enabling
	// TCP dial probes without an application-level probe is the common case.
	code, body = do(t, hs, "PUT", "/api/engine", "", map[string]any{
		"probes": map[string]any{"enabled": true, "http": map[string]any{"method": "GET"}},
	})
	require.Equal(t, http.StatusOK, code, string(body))
}
