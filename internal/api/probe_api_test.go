package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProber stands in for the router: it records the spec it was handed and
// returns canned results, so these tests cover the HTTP surface (decoding,
// validation, status codes) without dialing anything.
type fakeProber struct {
	specs []core.HTTPProbeSpec
	res   []core.HTTPProbe
	err   error
}

func (f *fakeProber) RunHTTPProbes(_ context.Context, spec core.HTTPProbeSpec) ([]core.HTTPProbe, error) {
	f.specs = append(f.specs, spec)
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
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

	require.Len(t, fp.specs, 1)
	spec := fp.specs[0]
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
