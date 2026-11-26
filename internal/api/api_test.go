package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/provider"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// newAPIServer builds an API server backed by a fresh config store and a temp
// metrics store. The returned httptest.Server is wired to the API's auth
// wrapper when a token is provided.
//
// These tests double as a check on the swag annotations in api.go: the
// error bodies asserted here (plain JSON {"error": "..."}) are the same
// errResponse shape the generated OpenAPI document declares.
func newAPIServer(t *testing.T, token string) (*httptest.Server, *config.Store, *metrics.Store) {
	t.Helper()
	cfg := config.NewStore()
	ms, err := metrics.Open(t.TempDir() + "/api.db")
	if err != nil {
		t.Fatalf("metrics.Open: %v", err)
	}
	t.Cleanup(func() { ms.Close() })
	srv := New(cfg, ms)
	hs := httptest.NewServer(srv.Auth(token))
	t.Cleanup(func() { hs.Close() })
	return hs, cfg, ms
}

// do issues a request with an optional bearer token and returns the status +
// body. It does NOT follow redirects.
func do(t *testing.T, hs *httptest.Server, method, path, token string, body any) (int, []byte) {
	t.Helper()
	var r *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r, err = http.NewRequest(method, hs.URL+path, bytes.NewReader(b))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		r.Header.Set("Content-Type", "application/json")
	} else {
		var err error
		r, err = http.NewRequest(method, hs.URL+path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	// Route through the real server (so auth + mux run) by sending via the
	// test server's client.
	resp, err := hs.Client().Do(r)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, rerr := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if rerr != nil {
			break
		}
	}
	return resp.StatusCode, buf
}

// --- auth ---

// TestAuthRejectsMissingToken asserts that a token-protected API rejects
// requests with no Authorization header.
func TestAuthRejectsMissingToken(t *testing.T) {
	hs, _, _ := newAPIServer(t, "secret")
	code, _ := do(t, hs, "GET", "/api/engine", "", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("missing token should yield 401, got %d", code)
	}
}

// TestAuthRejectsWrongToken asserts that a wrong token is rejected, and that
// the comparison is not a prefix match (a token that shares a prefix with the
// real one must still be rejected).
func TestAuthRejectsWrongToken(t *testing.T) {
	hs, _, _ := newAPIServer(t, "secret")
	// Shared prefix, different suffix.
	if code, _ := do(t, hs, "GET", "/api/engine", "secr", nil); code != http.StatusUnauthorized {
		t.Fatalf("prefix-only token should yield 401, got %d", code)
	}
	// Completely wrong.
	if code, _ := do(t, hs, "GET", "/api/engine", "wrong", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong token should yield 401, got %d", code)
	}
}

// TestAuthAcceptsCorrectToken asserts the happy path.
func TestAuthAcceptsCorrectToken(t *testing.T) {
	hs, _, _ := newAPIServer(t, "secret")
	code, _ := do(t, hs, "GET", "/api/engine", "secret", nil)
	if code != http.StatusOK {
		t.Fatalf("correct token should yield 200, got %d", code)
	}
}

// TestAuthDisabledWhenNoToken asserts that an empty token disables auth.
func TestAuthDisabledWhenNoToken(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "GET", "/api/engine", "", nil)
	if code != http.StatusOK {
		t.Fatalf("no-token API should be open (200), got %d", code)
	}
}

// --- engine validation ---

// TestEngineRejectsBadStack asserts that an unknown stack value is rejected
// with a 400 rather than being persisted and failing later during reconcile.
func TestEngineRejectsBadStack(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{Stack: "bogus"})
	if code != http.StatusBadRequest {
		t.Fatalf("bad stack should yield 400, got %d body=%s", code, body)
	}
}

// TestEngineAcceptsValidStacks asserts system and gvisor (and empty) are
// accepted.
func TestEngineAcceptsValidStacks(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	for _, stk := range []string{"system", "gvisor", ""} {
		code, _ := do(t, hs, "PUT", "/api/engine", "", config.Engine{Stack: stk})
		if code != http.StatusOK {
			t.Fatalf("stack %q should yield 200, got %d", stk, code)
		}
	}
}

// TestEngineRejectsNegativeLogSizes asserts a negative log budget is a 400
// (over-cap values clamp silently, but negatives are typos).
func TestEngineRejectsNegativeLogSizes(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	for _, lc := range []config.LogConfig{
		{File: "x.log", MaxSizeMB: -1},
		{File: "x.log", MaxTotalMB: -5},
	} {
		code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{Stack: "system", Log: lc})
		if code != http.StatusBadRequest {
			t.Fatalf("negative log size should yield 400, got %d body=%s", code, body)
		}
	}
}

// TestEngineRejectsLogOnDatabase asserts the log writer cannot be pointed at
// the metrics database (or its WAL sidecars): appending and rotation/pruning
// would corrupt it.
func TestEngineRejectsLogOnDatabase(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	for _, f := range []string{"metrics.db", "./metrics.db", "metrics.db-wal", "metrics.db-shm"} {
		code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{
			Stack: "system", DB: "metrics.db", Log: config.LogConfig{File: f},
		})
		if code != http.StatusBadRequest {
			t.Fatalf("log.file %q on the DB should yield 400, got %d body=%s", f, code, body)
		}
	}
	// A distinct path is fine.
	code, _ := do(t, hs, "PUT", "/api/engine", "", config.Engine{
		Stack: "system", DB: "metrics.db", Log: config.LogConfig{File: "logs/goose.log"},
	})
	if code != http.StatusOK {
		t.Fatalf("distinct log path should yield 200, got %d", code)
	}
}

// TestEngineRejectsLogOnLiveDatabase asserts the collision check protects the
// DB the engine actually OPENED, not just the incoming db field: the engine
// never reopens metrics on config edits, so a PUT that renames db while
// aiming log.file at the live DB must still be rejected.
func TestEngineRejectsLogOnLiveDatabase(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetRuntimePaths(config.RuntimePaths{LiveDB: "live.db"})
	code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{
		Stack: "system", DB: "renamed.db", Log: config.LogConfig{File: "live.db"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("log.file on the live DB should yield 400, got %d body=%s", code, body)
	}
}

// TestEngineRejectsLogOnPersistedConfig asserts the log writer cannot be
// pointed at the config file the persister writes through.
func TestEngineRejectsLogOnPersistedConfig(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetRuntimePaths(config.RuntimePaths{PersistPath: "/data/goose.json"})
	for _, f := range []string{"/data/goose.json", "/data/./goose.json"} {
		code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{
			Stack: "system", Log: config.LogConfig{File: f},
		})
		if code != http.StatusBadRequest {
			t.Fatalf("log.file %q on the persisted config should yield 400, got %d body=%s", f, code, body)
		}
	}
}

// TestEngineRejectsLogSymlinkToDatabase asserts a symlink alias of the DB
// cannot slip past the string-only path comparison.
func TestEngineRejectsLogSymlinkToDatabase(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	dir := t.TempDir()
	db := filepath.Join(dir, "metrics.db")
	if err := os.WriteFile(db, []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "innocent.log")
	if err := os.Symlink(db, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	cfg.SetRuntimePaths(config.RuntimePaths{LiveDB: db})
	code, body := do(t, hs, "PUT", "/api/engine", "", config.Engine{
		Stack: "system", DB: db, Log: config.LogConfig{File: link},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("symlink to the DB should yield 400, got %d body=%s", code, body)
	}
}

// TestEngineMethodNotAllowed asserts that a method not handled by /api/engine
// (e.g. DELETE) yields 405.
func TestEngineMethodNotAllowed(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "DELETE", "/api/engine", "", nil)
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /api/engine should yield 405, got %d", code)
	}
}

// --- inbounds: id mismatch + method checks ---

// TestInboundIDMismatchRejected asserts that POST /api/inbounds/foo with a
// body whose id is "bar" is rejected with 400, preventing silent
// creation/update of a different resource.
func TestInboundIDMismatchRejected(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	in := config.Inbound{ID: "bar", Protocol: "http", Listen: "127.0.0.1:0"}
	code, body := do(t, hs, "POST", "/api/inbounds/foo", "", in)
	if code != http.StatusBadRequest {
		t.Fatalf("id mismatch should yield 400, got %d body=%s", code, body)
	}
}

// TestInboundItemPathSetsID asserts that POST /api/inbounds/foo with no body
// id (or a matching one) creates the resource under id "foo".
func TestInboundItemPathSetsID(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	in := config.Inbound{Protocol: "http", Listen: "127.0.0.1:0"} // no ID
	code, _ := do(t, hs, "POST", "/api/inbounds/foo", "", in)
	if code != http.StatusCreated {
		t.Fatalf("item-path create should yield 201, got %d", code)
	}
	// Verify it was stored under "foo".
	code, body := do(t, hs, "GET", "/api/inbounds/foo", "", nil)
	if code != http.StatusOK {
		t.Fatalf("GET created inbound should yield 200, got %d", code)
	}
	var got config.Inbound
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if got.ID != "foo" {
		t.Fatalf("stored id should be %q (from URL), got %q", "foo", got.ID)
	}
}

// TestInboundDeleteNotFound asserts DELETE of a missing inbound yields 404.
func TestInboundDeleteNotFound(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "DELETE", "/api/inbounds/ghost", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("DELETE missing inbound should yield 404, got %d", code)
	}
}

// --- providers ---

// TestProviderCRUD covers the provider collection: create via item path,
// list, get, default-pool rule (an omitted pool_id becomes "default"), and
// delete (404 on the second delete).
func TestProviderCRUD(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")

	// An omitted pool_id defaults to the "default" pool rather than 400.
	code, body := do(t, hs, "POST", "/api/providers", "", config.ProviderSpec{
		ID: "p0", Provider: "subscription",
	})
	if code != http.StatusCreated {
		t.Fatalf("missing pool_id should default and yield 201, got %d body=%s", code, body)
	}
	var created config.ProviderSpec
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal created: %v body=%s", err, body)
	}
	if len(created.PoolIDs) != 1 || created.PoolIDs[0] != config.DefaultPoolID {
		t.Fatalf("omitted pool_ids should round-trip as [%q], got %v", config.DefaultPoolID, created.PoolIDs)
	}

	// Create via the item path with no body id.
	spec := config.ProviderSpec{
		Provider: "subscription",
		PoolIDs:  []string{"pool-sub"},
		Config:   map[string]any{"url": "https://example.com/sub"},
	}
	code, body = do(t, hs, "POST", "/api/providers/p1", "", spec)
	if code != http.StatusCreated {
		t.Fatalf("create should yield 201, got %d body=%s", code, body)
	}

	// It is listed.
	code, body = do(t, hs, "GET", "/api/providers", "", nil)
	if code != http.StatusOK {
		t.Fatalf("list should yield 200, got %d", code)
	}
	var list []config.ProviderSpec
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("unmarshal list: %v body=%s", err, body)
	}
	if len(list) != 2 {
		t.Fatalf("list should carry p0 + p1, got %+v", list)
	}
	var hasP1 bool
	for _, p := range list {
		if p.ID == "p1" {
			hasP1 = true
		}
	}
	if !hasP1 {
		t.Fatalf("list should carry p1, got %+v", list)
	}

	// It is fetchable by id, with the URL round-tripped.
	code, body = do(t, hs, "GET", "/api/providers/p1", "", nil)
	if code != http.StatusOK {
		t.Fatalf("get should yield 200, got %d", code)
	}
	var got config.ProviderSpec
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if got.ID != "p1" || got.Provider != "subscription" || len(got.PoolIDs) != 1 || got.PoolIDs[0] != "pool-sub" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if u, _ := got.Config["url"].(string); u != "https://example.com/sub" {
		t.Fatalf("config url mismatch: %v", got.Config)
	}

	// The store saw it too.
	if _, ok := cfg.Provider("p1"); !ok {
		t.Fatal("provider p1 missing from store")
	}

	// Delete: 204 then 404.
	code, _ = do(t, hs, "DELETE", "/api/providers/p1", "", nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete should yield 204, got %d", code)
	}
	code, _ = do(t, hs, "DELETE", "/api/providers/p1", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("second delete should yield 404, got %d", code)
	}
}

// TestProviderIDMismatchRejected asserts POST /api/providers/foo with a body
// id of "bar" is rejected with 400.
func TestProviderIDMismatchRejected(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	spec := config.ProviderSpec{ID: "bar", Provider: "subscription", PoolIDs: []string{"p"}}
	code, _ := do(t, hs, "POST", "/api/providers/foo", "", spec)
	if code != http.StatusBadRequest {
		t.Fatalf("id mismatch should yield 400, got %d", code)
	}
}

// --- body size limit ---

// TestBodySizeLimitRejected asserts that an oversized JSON body is rejected
// (413 or 400 from MaxBytesReader) rather than consuming unbounded memory.
func TestBodySizeLimitRejected(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	// Build a body just over 1 MiB.
	big := strings.Repeat("x", maxBodyBytes+1)
	code, _ := do(t, hs, "POST", "/api/inbounds", "", map[string]any{
		"id":       "big",
		"protocol": "http",
		"listen":   "127.0.0.1:0",
		"junk":     big,
	})
	if code != http.StatusBadRequest && code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body should yield 400/413, got %d", code)
	}
}

// --- metrics: method check + row cap ---

// TestMetricsMethodNotAllowed asserts non-GET on /api/metrics yields 405.
func TestMetricsMethodNotAllowed(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")
	code, _ := do(t, hs, "POST", "/api/metrics", "", nil)
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/metrics should yield 405, got %d", code)
	}
}

// TestMetricsCapsRows asserts that a huge ?n= is capped to maxMetricsRows and
// does not crash (regression for the parseInt overflow / makeslice panic).
func TestMetricsCapsRows(t *testing.T) {
	hs, _, ms := newAPIServer(t, "")
	// Record a few metrics so Recent has something to return.
	for i := 0; i < 3; i++ {
		_ = ms.Record(recordMetric())
	}
	// A value that overflows int if accumulated naively.
	code, body := do(t, hs, "GET", "/api/metrics?n=99999999999999999999", "", nil)
	if code != http.StatusOK {
		t.Fatalf("huge ?n= should yield 200 (capped), got %d body=%s", code, body)
	}
	// A value that's huge but representable as int — must be capped, not crash.
	code, body = do(t, hs, "GET", "/api/metrics?n=999999999", "", nil)
	if code != http.StatusOK {
		t.Fatalf("large ?n= should yield 200 (capped), got %d body=%s", code, body)
	}
}

// TestMetricsBadNIsIgnored asserts a non-numeric ?n= falls back to the default
// (100) rather than erroring.
func TestMetricsBadNIsIgnored(t *testing.T) {
	hs, _, ms := newAPIServer(t, "")
	_ = ms.Record(recordMetric())
	code, _ := do(t, hs, "GET", "/api/metrics?n=abc", "", nil)
	if code != http.StatusOK {
		t.Fatalf("non-numeric ?n= should yield 200 (default), got %d", code)
	}
}

// recordMetric builds a minimal valid RequestMetric for seeding the store.
func recordMetric() core.RequestMetric {
	return core.RequestMetric{
		ID:        "m",
		InboundID: "in",
		Network:   core.NetworkTCP,
		Target:    "example.com:80",
		Chain:     []string{"ob1"},
		Success:   true,
	}
}

// --- OpenAPI spec freshness ---

// TestOpenAPISpecInSync asserts that the committed OpenAPI document matches
// what swag would generate from the current annotations, using the already
// generated swagger.json committed at internal/api/docs. This is a cheap
// structural smoke test (the byte-exact check runs in CI with the pinned
// swag); it guards against accidental hand-edits of the spec.
func TestOpenAPISpecInSync(t *testing.T) {
	data, err := os.ReadFile("../../internal/api/docs/swagger.json")
	if err != nil {
		t.Skipf("spec not present: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("swagger.json is not valid JSON: %v", err)
	}
	if doc["swagger"] != "2.0" {
		t.Fatalf("spec must be swagger 2.0, got %v", doc["swagger"])
	}
	info, _ := doc["info"].(map[string]any)
	if info == nil || info["title"] != "goose admin API" {
		t.Fatalf("spec title missing/wrong: %v", info)
	}
	paths, _ := doc["paths"].(map[string]any)
	for _, p := range []string{
		"/api/engine", "/api/inbounds", "/api/outbounds",
		"/api/pools", "/api/chains", "/api/providers", "/api/metrics",
	} {
		if _, ok := paths[p]; !ok {
			t.Errorf("spec is missing path %s", p)
		}
	}
}

// fakeStatusSource stubs the provider-status seam for the endpoint test.
type fakeStatusSource struct {
	sts []provider.ProviderStatus
}

func (f *fakeStatusSource) Status() []provider.ProviderStatus { return f.sts }

// TestProviderStatusEndpoint verifies /api/providers/status: the wired
// status source's view is served with plugin reports intact, and an
// unwired server yields an empty list rather than an error. A provider
// spec named "status" is deliberately not routed here — a collision is
// resolved by renaming, documented at the route.
func TestProviderStatusEndpoint(t *testing.T) {
	// Build the server directly: the harness helper wires auth but has no
	// way to set the status source.
	store := config.NewStore()
	ms, err := metrics.Open(t.TempDir() + "/api-status.db")
	if err != nil {
		t.Fatalf("metrics.Open: %v", err)
	}
	t.Cleanup(func() { ms.Close() })
	srv := New(store, ms)

	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { hs.Close() })

	// Unwired: empty list, 200.
	code, body := do(t, hs, "GET", "/api/providers/status", "", nil)
	if code != http.StatusOK {
		t.Fatalf("unwired status: code %d, want 200", code)
	}
	if strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("unwired status body = %q, want []", body)
	}

	// Wired: the merged view is served verbatim, plugin report included.
	now := time.Now()
	srv.SetProviderStatusSource(&fakeStatusSource{sts: []provider.ProviderStatus{
		{
			ID:            "psiphon",
			Provider:      "psiphon",
			Pools:         []string{"default"},
			State:         "running",
			OutboundCount: 12,
			StartedAt:     now,
			LastPollAt:    now,
			LastOKAt:      now,
			Plugin: &pub.ProviderStatus{
				State:   "running",
				Details: map[string]any{"servers_in_list": 9},
			},
		},
	}})
	code, body = do(t, hs, "GET", "/api/providers/status", "", nil)
	if code != http.StatusOK {
		t.Fatalf("wired status: code %d, want 200", code)
	}
	var got []provider.ProviderStatus
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal status: %v (%s)", err, body)
	}
	if len(got) != 1 || got[0].ID != "psiphon" || got[0].OutboundCount != 12 {
		t.Fatalf("wired status = %s", body)
	}
	if got[0].Plugin == nil || got[0].Plugin.Details["servers_in_list"] != float64(9) {
		t.Fatalf("plugin report missing: %s", body)
	}

	// Method policing.
	code, _ = do(t, hs, "POST", "/api/providers/status", "", nil)
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status: code %d, want 405", code)
	}
}

// --- observed-path JSON surface ---
//
// A persisted row carries an immutable *core.PathGraph snapshot, so the JSON a
// client sees is the graph the engine observed — and a row recorded by an
// engine that could not observe one omits the key entirely rather than
// serializing null or an empty graph.

// apiPAddr is a fixed net.Addr for the inert conns an observed leg returns.
type apiPAddr string

func (a apiPAddr) Network() string { return "tcp" }
func (a apiPAddr) String() string  { return string(a) }

// apiPConn is the net.Conn an observed leg produced. Nothing reads or writes
// it; the recorder only asks for RemoteAddr, which is the socket peer a node
// adopts. An empty remote stands for a conn whose peer is not a dialable
// host:port (an in-memory tunnel conn), which must NOT be adopted.
type apiPConn struct{ remote net.Addr }

func (c apiPConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c apiPConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c apiPConn) Close() error                     { return nil }
func (c apiPConn) LocalAddr() net.Addr              { return apiPAddr("127.0.0.1:5555") }
func (c apiPConn) RemoteAddr() net.Addr             { return c.remote }
func (c apiPConn) SetDeadline(time.Time) error      { return nil }
func (c apiPConn) SetReadDeadline(time.Time) error  { return nil }
func (c apiPConn) SetWriteDeadline(time.Time) error { return nil }

var apiPErrRefused = errors.New("dial tcp 203.0.113.7:1080: connect: connection refused")

const (
	apiPTarget     = "example.com:443"
	apiPTargetHost = "example.com"
	apiPTargetPort = "443"

	apiPFirstAddr  = "203.0.113.7:1080" // failover attempt 1: refused
	apiPSecondAddr = "203.0.113.8:1080" // failover attempt 2: arrives

	apiPProxyID = "us-1"
	apiPPhase   = "connect"
	apiPReason  = "connect_refused"

	apiPIDLocal  = core.NodeLocalID
	apiPIDFirst  = core.NodeProxyPrefix + apiPProxyID
	apiPIDSecond = core.NodeProxyPrefix + apiPProxyID + "#2"
	apiPIDTarget = core.NodeTargetID
)

// apiPGraph builds a failover graph with a REAL recorder and returns its
// sealed snapshot — the same way a router row produces one. Attempt 1 is
// refused; attempt 2 arrives at its own server and handshakes toward the
// target inside the same bracketed call, so that one call yields TWO edges:
// the timed arrival, then a tunnel edge with Duration 0 and Observed true.
func apiPGraph() *core.PathGraph {
	rec := core.NewPathRecorder()
	rec.SetClassifier(func(core.LegInfo, error) (string, string) {
		return apiPPhase, apiPReason
	})
	rec.AddLocal()
	target := rec.AddTarget(apiPTargetHost, apiPTargetPort)

	first := rec.AddProxyAttempt(apiPProxyID, apiPFirstAddr, 1)
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegDial, To: first, Through: target, Target: apiPTarget,
	}, nil, func() (net.Conn, error) {
		return nil, apiPErrRefused
	})

	second := rec.AddProxyAttempt(apiPProxyID, apiPSecondAddr, 2)
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegDial, To: second, Through: target, Target: apiPTarget,
	}, nil, func() (net.Conn, error) {
		return apiPConn{remote: apiPAddr(apiPSecondAddr)}, nil
	})

	return rec.Snapshot()
}

// apiPDirectGraph builds the graph a "direct" route produces: local → target
// with no proxy node between them, because Direct has no server of its own.
func apiPDirectGraph() *core.PathGraph {
	rec := core.NewPathRecorder()
	rec.AddLocal()
	target := rec.AddTarget(apiPTargetHost, apiPTargetPort)
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegStack, To: target, Target: apiPTarget,
	}, nil, func() (net.Conn, error) {
		return apiPConn{remote: apiPAddr("")}, nil
	})
	return rec.Snapshot()
}

// apiPIDs projects a node list to its IDs in order.
func apiPIDs(nodes []core.PathNode) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

// apiPAssertGraph asserts the failover graph a client decoded off the wire.
func apiPAssertGraph(t *testing.T, g *core.PathGraph) {
	t.Helper()
	if g == nil {
		t.Fatal("decoded row carries no graph")
	}
	if len(g.Nodes) != 4 {
		t.Fatalf("nodes = %v, want local + both attempts + target", apiPIDs(g.Nodes))
	}
	if got, want := strings.Join(apiPIDs(g.Nodes), ","),
		strings.Join([]string{apiPIDLocal, apiPIDFirst, apiPIDSecond, apiPIDTarget}, ","); got != want {
		t.Errorf("node order = %s, want %s", got, want)
	}
	// One node per attempt, so the loser keeps its own address and attempt
	// number instead of being overwritten by the member that won.
	if n := g.Nodes[1]; n.Address != apiPFirstAddr || n.AddressSource != core.AddrConfigured ||
		n.Attempt != 1 || n.OutboundID != apiPProxyID {
		t.Errorf("attempt-1 node = %+v, want %s configured, attempt 1", n, apiPFirstAddr)
	}
	if n := g.Nodes[2]; n.Address != apiPSecondAddr || n.AddressSource != core.AddrSocket ||
		n.Attempt != 2 || n.OutboundID != apiPProxyID {
		t.Errorf("attempt-2 node = %+v, want %s off the socket, attempt 2", n, apiPSecondAddr)
	}
	if n := g.Nodes[3]; n.Kind != core.NodeTarget || n.Host != apiPTargetHost ||
		n.Port != apiPTargetPort || n.Address != apiPTarget {
		t.Errorf("target node = %+v, want %s/%s", n, apiPTargetHost, apiPTargetPort)
	}

	if len(g.Edges) != 3 {
		t.Fatalf("edges = %d, want the refusal + the arrival + the handshake", len(g.Edges))
	}
	lost := g.Edges[0]
	if lost.From != apiPIDLocal || lost.To != apiPIDFirst || lost.Kind != core.LegDial ||
		!lost.Observed || lost.Success || lost.Error != apiPErrRefused.Error() ||
		lost.Phase != apiPPhase || lost.Reason != apiPReason {
		t.Errorf("refused edge = %+v", lost)
	}
	won := g.Edges[1]
	if won.From != apiPIDLocal || won.To != apiPIDSecond || won.Kind != core.LegDial ||
		!won.Observed || !won.Success || won.Error != "" {
		t.Errorf("winning edge = %+v", won)
	}
	tunnel := g.Edges[2]
	if tunnel.From != apiPIDSecond || tunnel.To != apiPIDTarget || tunnel.Kind != core.LegTunnel ||
		!tunnel.Observed || !tunnel.Success {
		t.Errorf("handshake edge = %+v", tunnel)
	}
	if tunnel.Duration != 0 {
		t.Errorf("handshake duration = %v, want 0: the second leg of one bracketed call is not separately timed", tunnel.Duration)
	}
	if g.Partial {
		t.Error("a fully observed graph must not report Partial")
	}
	if g.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0", g.Dropped)
	}
}

// apiPAwait polls cond until it holds or the deadline passes. DB writes are
// batched asynchronously, so a test cannot read them back immediately.
func apiPAwait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// apiPRowsByIndex decodes an API array body into one raw object per element, so
// a test can assert on key PRESENCE (omitempty) rather than decoded zeros.
func apiPRowsByIndex(t *testing.T, body []byte) []map[string]json.RawMessage {
	t.Helper()
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	return rows
}

// TestMetricsJSONOmitsAbsentPath asserts a stored row with no observed graph
// serializes WITHOUT a "path" key — omitempty drops it, so an older engine's
// row and a row whose path could not be observed look the same to a client and
// neither decodes to an empty graph — while a row that carries one serializes
// "nodes" and "edges".
func TestMetricsJSONOmitsAbsentPath(t *testing.T) {
	// The value the metrics store marshals into bbolt.
	raw, err := json.Marshal(recordMetric())
	if err != nil {
		t.Fatalf("marshal path-less metric: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("unmarshal raw metric: %v", err)
	}
	if v, ok := keys["path"]; ok {
		t.Fatalf("a path-less row must omit \"path\" entirely, got %s in %s", v, raw)
	}
	if _, ok := keys["chain"]; !ok {
		t.Fatalf("sanity: the row itself serialized: %s", raw)
	}

	withPath := recordMetric()
	withPath.ID = "withpath"
	withPath.Path = apiPGraph()
	raw, err = json.Marshal(withPath)
	if err != nil {
		t.Fatalf("marshal metric with path: %v", err)
	}
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("unmarshal raw metric: %v", err)
	}
	graph, ok := keys["path"]
	if !ok {
		t.Fatalf("a row with a graph must serialize \"path\": %s", raw)
	}
	var graphKeys map[string]json.RawMessage
	if err := json.Unmarshal(graph, &graphKeys); err != nil {
		t.Fatalf("unmarshal path: %v", err)
	}
	for _, k := range []string{"nodes", "edges"} {
		if _, ok := graphKeys[k]; !ok {
			t.Errorf("graph is missing %q: %s", k, graph)
		}
	}

	// And the same holds on the wire: /api/metrics re-serializes the rows it
	// read back out of bbolt. Rows are keyed by StartedAt, so give them
	// explicit, ordered timestamps rather than the zero time both would share.
	hs, _, ms := newAPIServer(t, "")
	base := time.Now().Add(-time.Hour)
	noPath := recordMetric()
	noPath.ID = "nopath"
	noPath.StartedAt = base
	noPath.FinishedAt = base.Add(time.Millisecond)
	withPath.StartedAt = base.Add(-time.Minute)
	withPath.FinishedAt = base.Add(-time.Minute + time.Millisecond)
	if err := ms.Record(noPath); err != nil {
		t.Fatalf("record path-less row: %v", err)
	}
	if err := ms.Record(withPath); err != nil {
		t.Fatalf("record row with path: %v", err)
	}

	var body []byte
	apiPAwait(t, "both metric rows to surface", func() bool {
		code, b := do(t, hs, "GET", "/api/metrics", "", nil)
		if code != http.StatusOK {
			return false
		}
		body = b
		return len(apiPRowsByIndex(t, b)) == 2
	})

	// Newest first, so the path-less row leads.
	rows := apiPRowsByIndex(t, body)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if v, ok := rows[0]["path"]; ok {
		t.Errorf("served path-less row carries \"path\": %s", v)
	}
	if _, ok := rows[1]["path"]; !ok {
		t.Errorf("served row with a graph has no \"path\" key: %s", body)
	}

	var served []core.RequestMetric
	if err := json.Unmarshal(body, &served); err != nil {
		t.Fatalf("unmarshal /api/metrics body: %v", err)
	}
	if served[0].Path != nil {
		t.Errorf("path-less row decoded to %+v, want nil", served[0].Path)
	}
	apiPAssertGraph(t, served[1].Path)
}

// TestProbeResultsJSONCarriesPath asserts GET /api/probes/results round-trips
// the observed path: the graph the prober snapshotted reaches a client intact —
// nodes in path order with the target last, one node per failover attempt with
// its own address and address source, and the failed leg's classification —
// while a probe whose route had no proxy carries the bare local → target graph.
func TestProbeResultsJSONCarriesPath(t *testing.T) {
	hs, _, ms := newAPIServer(t, "")
	base := time.Now().Add(-time.Hour)

	proxied := core.HTTPProbe{
		ID: "p1", Route: "pool:default", Target: "https://example.com/ip", Method: "GET",
		Status: 200, Success: true, Latency: 30 * time.Millisecond,
		StartedAt: base, Path: apiPGraph(),
	}
	direct := core.HTTPProbe{
		ID: "p2", Route: "direct", Target: "https://example.com/ip", Method: "GET",
		Status: 200, Success: true, Latency: 10 * time.Millisecond,
		StartedAt: base.Add(time.Minute), Path: apiPDirectGraph(),
	}
	// A row from an engine that observed nothing: no "path" key on the wire.
	legacy := core.HTTPProbe{
		ID: "p3", Route: "outbound:us-1", Target: "https://example.com/ip", Method: "GET",
		Status: 502, Success: false, StartedAt: base.Add(2 * time.Minute),
	}
	for _, p := range []core.HTTPProbe{proxied, direct, legacy} {
		if err := ms.RecordHTTPProbe(p); err != nil {
			t.Fatalf("RecordHTTPProbe(%s): %v", p.ID, err)
		}
	}

	code, body := do(t, hs, "GET", "/api/probes/results", "", nil)
	if code != http.StatusOK {
		t.Fatalf("GET /api/probes/results = %d, want 200: %s", code, body)
	}

	rows := apiPRowsByIndex(t, body)
	if len(rows) != 3 {
		t.Fatalf("got %d probe rows, want 3: %s", len(rows), body)
	}
	// Newest first: legacy, direct, proxied.
	for i, id := range []string{"p3", "p2", "p1"} {
		var row core.HTTPProbe
		raw, err := json.Marshal(rows[i])
		if err != nil {
			t.Fatalf("re-marshal row %d: %v", i, err)
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatalf("unmarshal row %d: %v", i, err)
		}
		if row.ID != id {
			t.Errorf("row %d id = %q, want %q (newest first)", i, row.ID, id)
		}
	}

	// The row with no observed path omits the key entirely.
	if v, ok := rows[0]["path"]; ok {
		t.Errorf("a probe with no graph must omit \"path\", got %s", v)
	}
	for i := 1; i < 3; i++ {
		graph, ok := rows[i]["path"]
		if !ok {
			t.Fatalf("probe row %d has no \"path\": %s", i, body)
		}
		var graphKeys map[string]json.RawMessage
		if err := json.Unmarshal(graph, &graphKeys); err != nil {
			t.Fatalf("unmarshal probe path: %v", err)
		}
		for _, k := range []string{"nodes", "edges"} {
			if _, ok := graphKeys[k]; !ok {
				t.Errorf("probe row %d graph is missing %q: %s", i, k, graph)
			}
		}
	}

	var served []core.HTTPProbe
	if err := json.Unmarshal(body, &served); err != nil {
		t.Fatalf("unmarshal /api/probes/results body: %v", err)
	}
	if served[0].Path != nil {
		t.Errorf("legacy probe decoded to %+v, want a nil path", served[0].Path)
	}

	// "direct" has no server, so its graph is just the engine host and the
	// target with one stack leg between them.
	g := served[1].Path
	if g == nil {
		t.Fatal("the direct probe lost its graph")
	}
	if got, want := strings.Join(apiPIDs(g.Nodes), ","), apiPIDLocal+","+apiPIDTarget; got != want {
		t.Errorf("direct graph nodes = %s, want %s", got, want)
	}
	if len(g.Edges) != 1 {
		t.Fatalf("direct graph edges = %d, want 1", len(g.Edges))
	}
	if e := g.Edges[0]; e.From != apiPIDLocal || e.To != apiPIDTarget ||
		e.Kind != core.LegStack || !e.Observed || !e.Success {
		t.Errorf("direct graph edge = %+v", e)
	}
	if g.Partial {
		t.Error("a direct graph must not report Partial")
	}

	apiPAssertGraph(t, served[2].Path)

	// The route filter keeps the graph.
	code, body = do(t, hs, "GET", "/api/probes/results?route=pool:default", "", nil)
	if code != http.StatusOK {
		t.Fatalf("filtered GET = %d, want 200: %s", code, body)
	}
	var filtered []core.HTTPProbe
	if err := json.Unmarshal(body, &filtered); err != nil {
		t.Fatalf("unmarshal filtered body: %v", err)
	}
	if len(filtered) != 1 || filtered[0].ID != "p1" {
		t.Fatalf("filtered rows = %+v, want only p1", filtered)
	}
	apiPAssertGraph(t, filtered[0].Path)
}
