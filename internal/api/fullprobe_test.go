package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFullRunner stands in for the router's full-probe runner: it records the
// spec it was handed and serves canned job snapshots, so these tests cover
// the HTTP surface (decoding, validation, status codes, config writes)
// without dialing anything.
type fakeFullRunner struct {
	mu    sync.Mutex
	specs []core.FullProbeSpec
	jobs  map[string]*core.FullProbeJob
	err   error
}

func (f *fakeFullRunner) StartFullProbe(spec core.FullProbeSpec) (*core.FullProbeJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.specs = append(f.specs, spec)
	if f.err != nil {
		return nil, f.err
	}
	j := &core.FullProbeJob{
		ID: "fp-test", Spec: spec, State: core.FullProbeRunning, StartedAt: time.Now(),
	}
	if f.jobs == nil {
		f.jobs = map[string]*core.FullProbeJob{}
	}
	f.jobs[j.ID] = j
	return j.Clone(), nil
}

func (f *fakeFullRunner) FullProbeJob(id string) (*core.FullProbeJob, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, false
	}
	return j.Clone(), true
}

func (f *fakeFullRunner) FullProbeJobs(n int) []*core.FullProbeJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*core.FullProbeJob
	for _, j := range f.jobs {
		out = append(out, j.Clone())
	}
	if len(out) > n {
		out = out[:n]
	}
	return out
}

func (f *fakeFullRunner) CancelFullProbe(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok || j.State != core.FullProbeRunning {
		return false
	}
	j.State = core.FullProbeCanceled
	j.FinishedAt = time.Now()
	return true
}

func (f *fakeFullRunner) set(j *core.FullProbeJob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobs == nil {
		f.jobs = map[string]*core.FullProbeJob{}
	}
	f.jobs[j.ID] = j
}

// newFullProbeServer wires an API server with the fake runner and a config
// holding the relay/work outbounds the canned job's paths reference.
func newFullProbeServer(t *testing.T, fr *fakeFullRunner) (*httptest.Server, *config.Store, *metrics.Store) {
	t.Helper()
	_, cfg, ms := newAPIServer(t, "")
	cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5"})
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5"})
	srv := New(cfg, ms)
	if fr != nil {
		srv.SetFullProbeRunner(fr)
	}
	hs := httptest.NewServer(srv.Auth(""))
	t.Cleanup(hs.Close)
	return hs, cfg, ms
}

// finishedJob is the canned two-level rescue result: depth 1 relay works,
// depth 2 relay→work works.
func finishedJob() *core.FullProbeJob {
	return &core.FullProbeJob{
		ID: "fp-test", State: core.FullProbeDone, StartedAt: time.Now(),
		Spec: core.FullProbeSpec{Target: "http://example.com"},
		Levels: []core.FullProbeLevel{
			{Depth: 1, SuccessPaths: []core.FullProbePath{
				{Hops: []string{"relay"}, Latency: 10 * time.Millisecond, Status: 200},
			}, FailedItems: []core.FullProbeFailedItem{{ID: "work", Count: 1}}},
			{Depth: 2, SuccessPaths: []core.FullProbePath{
				{Hops: []string{"relay", "work"}, Latency: 20 * time.Millisecond, Status: 200},
			}},
		},
	}
}

// TestFullProbeStartDecodesSpec asserts POST /api/probes/full maps the body
// (seconds-based timeout) onto core.FullProbeSpec and answers 201 with the
// job snapshot.
func TestFullProbeStartDecodesSpec(t *testing.T) {
	fr := &fakeFullRunner{}
	hs, _, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full", "", map[string]any{
		"target": "https://example.com/ip", "method": "POST",
		"headers": map[string]string{"X-T": "1"}, "body": `{"a":1}`,
		"timeout_seconds": 7, "max_depth": 3, "rps": 5000, "concurrency": 64,
		"max_attempts_per_level": 500, "max_paths_per_level": 32, "max_total_attempts": 9000,
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	var j core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &j))
	assert.Equal(t, "fp-test", j.ID)
	assert.Equal(t, core.FullProbeRunning, j.State)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	require.Len(t, fr.specs, 1)
	s := fr.specs[0]
	assert.Equal(t, "https://example.com/ip", s.Target)
	assert.Equal(t, "POST", s.Method)
	assert.Equal(t, 7*time.Second, s.Timeout)
	assert.Equal(t, 3, s.MaxDepth)
	assert.Equal(t, 5000, s.RPS)
	assert.Equal(t, 64, s.Concurrency)
	assert.Equal(t, 500, s.MaxAttemptsPerLevel)
	assert.Equal(t, 32, s.MaxPathsPerLevel)
	assert.Equal(t, 9000, s.MaxTotalAttempts)
}

// TestFullProbeStartRejections covers the start endpoint's failure surface:
// no body, an unrunnable spec, an unwired runner, and a wrong method.
func TestFullProbeStartRejections(t *testing.T) {
	hs, _, _ := newFullProbeServer(t, &fakeFullRunner{})
	code, _ := do(t, hs, "POST", "/api/probes/full", "", nil)
	assert.Equal(t, http.StatusBadRequest, code, "missing body")
	code, _ = do(t, hs, "POST", "/api/probes/full", "", map[string]any{"target": "http://x.com", "bogus": 1})
	assert.Equal(t, http.StatusBadRequest, code, "unknown field")

	bad := &fakeFullRunner{err: assert.AnError}
	hs2, _, _ := newFullProbeServer(t, bad)
	code, _ = do(t, hs2, "POST", "/api/probes/full", "", map[string]any{"target": "http://x.com"})
	assert.Equal(t, http.StatusBadRequest, code, "runner rejection")

	unwired, _, _ := newFullProbeServer(t, nil)
	code, _ = do(t, unwired, "POST", "/api/probes/full", "", map[string]any{"target": "http://x.com"})
	assert.Equal(t, http.StatusServiceUnavailable, code, "unwired runner")
	code, _ = do(t, hs, "PUT", "/api/probes/full", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
}

// TestFullProbeGetListCancel covers the job views: GET serves the live
// snapshot, the list merges runner jobs, DELETE cancels a running job (204)
// and reports 409 once finished, 404 for an unknown ID.
func TestFullProbeGetListCancel(t *testing.T) {
	fr := &fakeFullRunner{}
	hs, _, _ := newFullProbeServer(t, fr)
	j0, err := fr.StartFullProbe(core.FullProbeSpec{Target: "http://example.com"})
	require.NoError(t, err)

	code, body := do(t, hs, "GET", "/api/probes/full/"+j0.ID, "", nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var j core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &j))
	assert.Equal(t, core.FullProbeRunning, j.State)

	code, body = do(t, hs, "GET", "/api/probes/full?n=5", "", nil)
	require.Equal(t, http.StatusOK, code)
	var list []*core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &list))
	require.Len(t, list, 1)

	code, _ = do(t, hs, "DELETE", "/api/probes/full/"+j0.ID, "", nil)
	assert.Equal(t, http.StatusNoContent, code)
	code, _ = do(t, hs, "DELETE", "/api/probes/full/"+j0.ID, "", nil)
	assert.Equal(t, http.StatusConflict, code, "finished job cannot be canceled")
	code, _ = do(t, hs, "DELETE", "/api/probes/full/fp-absent", "", nil)
	assert.Equal(t, http.StatusNotFound, code)
	code, _ = do(t, hs, "GET", "/api/probes/full/fp-absent", "", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// TestFullProbeDurableFallback asserts the job views fall back to the metrics
// history when the runner does not know the job (an engine restart), and the
// list serves persisted history when no runner is wired at all.
func TestFullProbeDurableFallback(t *testing.T) {
	hs, _, ms := newFullProbeServer(t, &fakeFullRunner{})
	require.NoError(t, ms.RecordFullProbeJob(finishedJob()))
	require.Eventually(t, func() bool {
		_, ok := ms.FullProbeJob("fp-test")
		return ok
	}, 5*time.Second, 20*time.Millisecond)

	code, body := do(t, hs, "GET", "/api/probes/full/fp-test", "", nil)
	require.Equal(t, http.StatusOK, code, string(body))

	unwired, _, ms2 := newFullProbeServer(t, nil)
	require.NoError(t, ms2.RecordFullProbeJob(finishedJob()))
	require.Eventually(t, func() bool {
		_, ok := ms2.FullProbeJob("fp-test")
		return ok
	}, 5*time.Second, 20*time.Millisecond)
	code, body = do(t, unwired, "GET", "/api/probes/full", "", nil)
	require.Equal(t, http.StatusOK, code)
	var list []*core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &list))
	require.Len(t, list, 1)

	code, _ = do(t, unwired, "DELETE", "/api/probes/full/fp-test", "", nil)
	assert.Equal(t, http.StatusServiceUnavailable, code)
}

// TestFullProbeApplyDefaults asserts the no-body apply builds BestPaths — one
// chain + inbound per depth — into the live config with concrete free ports.
func TestFullProbeApplyDefaults(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", nil)
	require.Equal(t, http.StatusCreated, code, string(body))
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "fp-test", resp.JobID)
	require.Len(t, resp.Results, 2)

	for i, res := range resp.Results {
		wantHops := [][]string{{"relay"}, {"relay", "work"}}[i]
		assert.Equal(t, wantHops, res.Path.Hops)
		assert.Equal(t, "socks5", res.Protocol)
		assert.True(t, strings.HasPrefix(res.Listen, "127.0.0.1:"), "listen %q", res.Listen)
		portStr := strings.TrimPrefix(res.Listen, "127.0.0.1:")
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)
		assert.Greater(t, port, 0, "a concrete free port must be named")

		ch, ok := cfg.Chain(res.ChainID)
		require.True(t, ok, "chain %s missing from config", res.ChainID)
		assert.Equal(t, wantHops, ch.Layers)
		in, ok := cfg.Inbound(res.InboundID)
		require.True(t, ok, "inbound %s missing from config", res.InboundID)
		assert.Equal(t, "socks5", in.Protocol)
		assert.Equal(t, res.Listen, in.Listen)
		assert.Equal(t, res.ChainID, in.Policy.ChainID)
		assert.Contains(t, res.ChainID, "fullprobe-fp-test-d")
	}
}

// TestFullProbeApplyExplicit asserts an explicit selection applies exactly
// the named proven paths, with the requested protocol/port/id_prefix.
func TestFullProbeApplyExplicit(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"paths":         [][]string{{"relay", "work"}},
		"protocol":      "http",
		"port":          23100,
		"id_prefix":     "rescue",
		"listen_prefix": "127.0.0.1",
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Results, 1)
	res := resp.Results[0]
	assert.Equal(t, "rescue-d2-1", res.ChainID)
	assert.Equal(t, "rescue-d2-1", res.InboundID)
	assert.Equal(t, "127.0.0.1:23100", res.Listen)
	assert.Equal(t, "http", res.Protocol)
	ch, ok := cfg.Chain("rescue-d2-1")
	require.True(t, ok)
	assert.Equal(t, []string{"relay", "work"}, ch.Layers)
}

// TestFullProbeApplyRejections covers the apply endpoint's guards: unknown
// job, running job, unproven path, missing outbound, bad protocol/port, and
// an ID collision (which must not write anything).
func TestFullProbeApplyRejections(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	code, _ := do(t, hs, "POST", "/api/probes/full/fp-absent/inbounds", "", nil)
	assert.Equal(t, http.StatusNotFound, code)

	running := finishedJob()
	running.ID = "fp-running"
	running.State = core.FullProbeRunning
	fr.set(running)
	code, _ = do(t, hs, "POST", "/api/probes/full/fp-running/inbounds", "", nil)
	assert.Equal(t, http.StatusConflict, code)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"paths": [][]string{{"work"}}, // failed at depth 1, never proved
	})
	assert.Equal(t, http.StatusBadRequest, code, string(body))

	code, _ = do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"protocol": "vmess",
	})
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"port": 70000,
	})
	assert.Equal(t, http.StatusBadRequest, code)

	// An outbound deleted after the sweep: apply must refuse, not write.
	cfg.DeleteOutbound("work")
	code, body = do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", nil)
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Empty(t, cfg.Chains(), "a rejected apply must not write partial config")
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5"})

	// ID collision: pre-create the chain the default apply would generate.
	cfg.SetChain(&config.ChainSpec{ID: "fullprobe-fp-test-d1-1", Layers: []string{"relay"}})
	code, body = do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", nil)
	assert.Equal(t, http.StatusConflict, code, string(body))
	assert.Len(t, cfg.Chains(), 1, "collision must not write anything")
	assert.Empty(t, cfg.Inbounds())

	code, _ = do(t, hs, "GET", "/api/probes/full/fp-test/inbounds", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
}

// TestFullProbeApplyNoPaths asserts a finished job with no successful paths
// answers 400 — there is nothing to build.
func TestFullProbeApplyNoPaths(t *testing.T) {
	fr := &fakeFullRunner{}
	empty := finishedJob()
	empty.Levels = nil
	fr.set(empty)
	hs, _, _ := newFullProbeServer(t, fr)
	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", nil)
	assert.Equal(t, http.StatusBadRequest, code, string(body))
}
