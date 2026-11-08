// HTTP reachability probe tests: a probe issues a real HTTP request toward a
// target through a chosen route and records the response. The topology is the
// reachability fixture's — a SOCKS5 relay the engine can dial, a work server
// the local network may or may not reach — with a local HTTP server as the
// target, so "does traffic through this route work end to end" is answered by
// a real response rather than a handshake.
package router

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/stack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// probeTarget is an HTTP server that echoes what it received, so a probe's
// request (method, headers, body) is observable in the response a probe
// records.
func probeTarget(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Echo-Method", r.Method)
		w.Header().Set("X-Echo-Token", r.Header.Get("X-Token"))
		w.Header().Set("X-Echo-Agent", r.Header.Get("User-Agent"))
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "method=%s path=%s body=%s", r.Method, r.URL.Path, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newProbeOnlyRouter builds a router whose dial sweep is disabled and which
// has one SOCKS5 outbound, so the only metric rows in play are HTTP probe
// rows. That isolation is what makes "probe rows never set a reachability
// verdict" observable.
func newProbeOnlyRouter(t *testing.T) (*Router, *metrics.Store) {
	t.Helper()
	registerTestPlugins()
	addr := socks5Server(t)
	stk, err := stack.New("system")
	require.NoError(t, err)
	t.Cleanup(func() { stk.Close() })
	cfg := config.NewStore() // Probes zero value: no dial sweep
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5", Config: map[string]any{"address": addr}})
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	r := New(cfg, ms, stk)
	t.Cleanup(r.Close)
	return r, ms
}

// TestHTTPProbeThroughOutbound verifies the core case: a request toward a
// target through one outbound really rides that outbound's tunnel, and the
// recorded result carries the response.
func TestHTTPProbeThroughOutbound(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target.URL + "/ip"},
		Routes:  []string{"outbound:relay"},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)

	p := res[0]
	assert.True(t, p.Success, "probe through a working route should get a response: %s", p.Error)
	assert.Equal(t, http.StatusOK, p.Status)
	assert.Equal(t, "outbound:relay", p.Route)
	assert.Equal(t, target.URL+"/ip", p.Target)
	assert.Equal(t, "GET", p.Method)
	assert.Contains(t, p.Body, "method=GET path=/ip")
	assert.Equal(t, "goose-probe/1.0", p.Headers["X-Echo-Agent"], "a default User-Agent is supplied")
	assert.Greater(t, p.Latency, time.Duration(0))
	assert.False(t, p.StartedAt.IsZero())

	// The result is durable before the call returns, so a caller that times
	// out on the response still finds the row.
	rows, err := f.ms.RecentHTTPProbes(10, "outbound:relay")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, p.ID, rows[0].ID)
}

// TestHTTPProbeSendsMethodHeadersBody verifies the spec's request reaches the
// target verbatim: method, custom headers and body.
func TestHTTPProbeSendsMethodHeadersBody(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target.URL + "/submit"},
		Method:  "POST",
		Headers: map[string]string{"X-Token": "sekret", "Content-Type": "application/json"},
		Body:    `{"a":1}`,
		Routes:  []string{"outbound:relay"},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)

	p := res[0]
	require.True(t, p.Success, p.Error)
	assert.Equal(t, "POST", p.Method)
	assert.Equal(t, "sekret", p.Headers["X-Echo-Token"])
	assert.Contains(t, p.Body, `method=POST path=/submit body={"a":1}`)
}

// TestHTTPProbeFanout verifies one round covers every (target, route) pair:
// targets in spec order, routes in expanded order, one result each.
func TestHTTPProbeFanout(t *testing.T) {
	f := newReachFixture(t, false)
	t1, t2 := probeTarget(t), probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{t1.URL + "/one", t2.URL + "/two"},
		Routes:  []string{"outbound:relay", "direct"},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 4)

	var got []string
	for _, p := range res {
		got = append(got, p.Route+" "+p.Target)
		assert.True(t, p.Success, "%s via %s: %s", p.Target, p.Route, p.Error)
	}
	assert.Equal(t, []string{
		"outbound:relay " + t1.URL + "/one",
		"direct " + t1.URL + "/one",
		"outbound:relay " + t2.URL + "/two",
		"direct " + t2.URL + "/two",
	}, got)
}

// TestHTTPProbeDefaultsToEveryOutbound verifies an empty route list probes
// every built outbound, and that a route which cannot be dialed yields a
// failed result rather than an error for the whole round — an unreachable
// route is exactly what a probe is for.
func TestHTTPProbeDefaultsToEveryOutbound(t *testing.T) {
	f := newReachFixture(t, true) // the local network blocks the work server
	target := probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target.URL},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 2, "one result per built outbound")

	byRoute := map[string]core.HTTPProbe{}
	for _, p := range res {
		byRoute[p.Route] = p
	}
	require.Contains(t, byRoute, "outbound:relay")
	require.Contains(t, byRoute, "outbound:work")
	assert.True(t, byRoute["outbound:relay"].Success, byRoute["outbound:relay"].Error)
	assert.False(t, byRoute["outbound:work"].Success, "a blocked route must fail")
	assert.Contains(t, byRoute["outbound:work"].Error, "blocked by local network")
	assert.Zero(t, byRoute["outbound:work"].Status, "no response arrived")
}

// TestHTTPProbeThroughPool verifies a route reference that is not a single
// outbound — a pool — is probed through the pool's own selection path, the
// same one dispatch uses.
func TestHTTPProbeThroughPool(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target.URL},
		Routes:  []string{"pool:relays"},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.True(t, res[0].Success, res[0].Error)
	assert.Equal(t, "pool:relays", res[0].Route)
}

// TestHTTPProbeTruncatesBody verifies the body cap is reported as a fact: a
// body longer than MaxBodyBytes is cut to it and Truncated is set, so a regex
// that misses knows why.
func TestHTTPProbeTruncatesBody(t *testing.T) {
	f := newReachFixture(t, false)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat("x", 5000))
	}))
	t.Cleanup(srv.Close)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets:      []string{srv.URL},
		Routes:       []string{"direct"},
		Timeout:      10 * time.Second,
		MaxBodyBytes: 128,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.True(t, res[0].Success, res[0].Error)
	assert.True(t, res[0].Truncated)
	assert.Len(t, res[0].Body, 128)
}

// TestHTTPProbeRejectsUnrunnableSpec verifies the spec errors: a target that
// is not an absolute http(s) URL, an empty target list, and a fan-out beyond
// the round limit.
func TestHTTPProbeRejectsUnrunnableSpec(t *testing.T) {
	f := newReachFixture(t, false)
	ok := probeTarget(t)

	for _, targets := range [][]string{
		{},
		{"example.com/ip"},
		{"ftp://example.com/ip"},
		{ok.URL, "://bad"},
	} {
		_, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
			Targets: targets,
			Routes:  []string{"direct"},
		})
		assert.Error(t, err, "targets %v must be rejected", targets)
	}

	// 33 unique targets exceeds core.MaxProbeTargets (32).
	many := make([]string, 0, core.MaxProbeTargets+1)
	for i := range core.MaxProbeTargets + 1 {
		many = append(many, fmt.Sprintf("%s/%d", ok.URL, i))
	}
	_, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{Targets: many, Routes: []string{"direct"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

// TestHTTPProbeDedupesTargets verifies a repeated target is probed once, so a
// pasted list with duplicates does not double the round.
func TestHTTPProbeDedupesTargets(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	res, err := f.router.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target.URL + "/x", " " + target.URL + "/x ", target.URL + "/x"},
		Routes:  []string{"direct"},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
}

// TestHTTPProbeRowsStayInformational verifies the poisoning hazard is closed:
// a probe that fails because of the TARGET (a closed port) leaves the route's
// reachability unmeasured, so routing does not drop a healthy outbound over
// it. The metric row is still recorded, for display.
func TestHTTPProbeRowsStayInformational(t *testing.T) {
	r, ms := newProbeOnlyRouter(t)

	res, err := r.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{"http://127.0.0.1:1/ip"}, // nothing listens there
		Routes:  []string{"outbound:work"},
		Timeout: 5 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.False(t, res[0].Success)
	assert.NotEmpty(t, res[0].Error)

	_, _, measured := ms.Reachability("work")
	assert.False(t, measured, "a failed HTTP probe must not produce a reachability verdict")
	assert.Empty(t, ms.Stats("work").Samples, "a probe row must not feed an outbound's latency stats")

	// The row exists, tagged as a probe row, so the metrics table shows it.
	require.Eventually(t, func() bool {
		rows, err := ms.Recent(50)
		require.NoError(t, err)
		for _, m := range rows {
			if len(m.Chain) == 2 && m.Chain[0] == core.HopProbe && m.Chain[1] == "outbound:work" {
				assert.Equal(t, "127.0.0.1:1", m.Target, "the row's target is the probed host:port")
				assert.False(t, m.Success)
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "probe metric row never landed")
}

// TestScheduledHTTPProbeRound verifies the engine config drives a scheduled
// round: with probes.http configured, the probe loop issues it and the
// results land in the durable history without anyone calling RunHTTPProbes.
func TestScheduledHTTPProbeRound(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	eng := f.cfg.Engine()
	eng.Probes.HTTP = &config.HTTPProbeConfig{
		Targets:    []string{target.URL + "/scheduled"},
		Method:     "HEAD",
		Routes:     []string{"outbound:relay"},
		TimeoutSec: 10,
	}
	f.cfg.SetEngine(eng)
	f.router.Rebuild()

	require.Eventually(t, func() bool {
		rows, err := f.ms.RecentHTTPProbes(20, "outbound:relay")
		if err != nil || len(rows) == 0 {
			return false
		}
		for _, p := range rows {
			if strings.HasSuffix(p.Target, "/scheduled") && p.Method == "HEAD" {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "scheduled HTTP probe round never ran")
}

// TestScheduledHTTPProbeIgnoredWithoutTargets verifies an HTTP probe config
// with no targets is ignored rather than run as an empty (erroring) round:
// enabling the TCP dial sweep without an application-level probe is the
// common case.
func TestScheduledHTTPProbeIgnoredWithoutTargets(t *testing.T) {
	f := newReachFixture(t, false)
	eng := f.cfg.Engine()
	eng.Probes.HTTP = &config.HTTPProbeConfig{Method: "GET", Routes: []string{"direct"}}
	f.cfg.SetEngine(eng)
	f.router.Rebuild()

	f.awaitVerdict(t, "relay") // the dial sweep ran, so the round certainly did
	time.Sleep(200 * time.Millisecond)
	rows, err := f.ms.RecentHTTPProbes(20, "")
	require.NoError(t, err)
	assert.Empty(t, rows, "no HTTP probe records without targets")
}

// TestHTTPProbeHonorsSpecHost verifies a spec Host reaches the wire: probing
// an origin by address with a vhost Host is a normal operator move, and
// net/http silently drops a Host left in the header map.
func TestHTTPProbeHonorsSpecHost(t *testing.T) {
	r, _ := newProbeOnlyRouter(t)
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		got <- req.Host
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	res, err := r.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{srv.URL + "/ip"},
		Routes:  []string{"direct"},
		Headers: map[string]string{"Host": "vhost.example.com"},
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, res[0].Success, "probe should get a response: %s", res[0].Error)
	assert.Equal(t, "vhost.example.com", <-got)
}

// TestHTTPProbeDecodesCallerAcceptEncoding verifies a caller-set
// Accept-Encoding — which switches the transport's transparent decode off —
// still yields the plaintext body for assertions and the history pane.
func TestHTTPProbeDecodesCallerAcceptEncoding(t *testing.T) {
	r, _ := newProbeOnlyRouter(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			fmt.Fprint(w, "PLAINTEXT-MARKER")
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		fmt.Fprint(gz, "PLAINTEXT-MARKER")
		_ = gz.Close()
	}))
	t.Cleanup(srv.Close)

	res, err := r.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{srv.URL + "/ip"},
		Routes:  []string{"direct"},
		Headers: map[string]string{"Accept-Encoding": "gzip"},
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, res[0].Success, "probe should get a response: %s", res[0].Error)
	assert.Contains(t, res[0].Body, "PLAINTEXT-MARKER", "a forced Accept-Encoding must not record compressed bytes")
}

// TestHTTPProbeUnissuedJobLeavesNoRow pins the assemble/record split: a job
// the round's end cancelled before it was issued is reported to the caller
// but never stands in history as a failed probe.
func TestHTTPProbeUnissuedJobLeavesNoRow(t *testing.T) {
	_, ms := newProbeOnlyRouter(t)
	p := assembleProbe(core.HTTPProbeSpec{Method: "GET"}, "http://example.com/ip", "direct",
		time.Now(), 0, nil, "", false, context.Canceled)
	assert.False(t, p.Success)
	assert.Contains(t, p.Error, "context canceled")

	rows, err := ms.RecentHTTPProbes(10, "")
	require.NoError(t, err)
	assert.Empty(t, rows, "a request that never left must not be recorded")
}
