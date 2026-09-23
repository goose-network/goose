package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
)

// newAPIServer builds an API server backed by a fresh config store and a temp
// metrics store. The returned httptest.Server is wired to the API's auth
// wrapper when a token is provided.
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
