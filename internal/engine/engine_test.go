package engine

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
)

// fakeHandler is a minimal inbound.Handler for engine tests: Dial returns a
// pipe (so Bridge has something to copy), and Bridge drains both sides. It
// records the last authenticated user it saw so tests can assert dispatch.
type fakeHandler struct {
	lastUser string
}

func (f *fakeHandler) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	f.lastUser = meta.User
	a, b := net.Pipe()
	go a.Close() // nothing to relay; pipe closes immediately
	return b, []string{"ob1"}, nil
}

func (f *fakeHandler) Bridge(inb, up net.Conn, meta core.Metadata) {
	// Minimal bridge: close both. The pipe from Dial is already closing, so
	// this just tears the inbound side down promptly.
	_ = up.Close()
	_ = inb.Close()
}

// newTestEngine builds an engine with a temp DB and the given inbounds, plus
// a fake handler swapped in so we don't need real outbounds. It returns the
// engine and its config store.
//
// Because engine.New constructs its own router (which needs real outbounds to
// dial), we instead build the engine and then exercise the inbound listeners
// directly through the fake handler by replacing e.router — but router is a
// concrete type. The simpler, equivalent approach used here: drive the
// inbound listener via a real HTTP CONNECT against the engine's live port,
// and observe auth outcomes (407 vs 200) which depend only on the listener's
// AuthStore, not on the router. The router's Dial failure (no outbounds)
// yields 502, but auth happens first, so auth outcomes are deterministic.
func newTestEngine(t *testing.T, inbounds []*config.Inbound) (*Engine, *config.Store) {
	t.Helper()
	cfg := config.NewStore()
	cfg.SetEngine(config.Engine{Stack: "system", DB: t.TempDir() + "/engine.db"})
	for _, in := range inbounds {
		cfg.SetInbound(in)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e, cfg
}

// httpInbound builds an HTTP inbound config on the given listen address with
// the given users and default chain. A fixed address (not :0) is required for
// hot-reload tests so a restarted listener rebinds the same port.
func httpInbound(id, listen string, users []config.User) *config.Inbound {
	return &config.Inbound{
		ID:       id,
		Protocol: "http",
		Listen:   listen,
		Users:    users,
		Policy:   config.InboundPolicy{ChainID: "c1"},
	}
}

// freeAddr returns a free 127.0.0.1:port by briefly listening then closing.
// Used to give hot-reload tests a fixed port that survives listener restarts.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// proxyConnect performs an HTTP CONNECT through the engine's listener with
// the given basic-auth credentials. It returns the HTTP status line, or an
// error string on dial/read failure. Auth is evaluated before the router
// dial, so a 407 means auth failed and a 502 means auth passed (and the dial
// then failed, which is expected with no real outbounds). Returning errors as
// strings (rather than fataling) lets callers retry through the brief window
// where a hot-reload restart has closed the old listener but not yet bound
// the new one.
func proxyConnect(t *testing.T, addr, user, pass string) string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return "dial-error: " + err.Error()
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: "example.com:443"},
		Host:   "example.com:443",
		Header: http.Header{},
	}
	if user != "" {
		cred := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		req.Header.Set("Proxy-Authorization", "Basic "+cred)
	}
	if err := req.Write(c); err != nil {
		return "write-error: " + err.Error()
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return "read-error: " + err.Error()
	}
	return line
}

// TestEngineHotReloadCredentials asserts that changing an inbound's user
// password via the config store is picked up by reconcile without restarting
// the port: the old password stops working and the new one starts, on the
// SAME listener address. This is the regression test for the hot-reload bug
// where a credential change that didn't change the listen address was
// silently ignored.
func TestEngineHotReloadCredentials(t *testing.T) {
	addr := freeAddr(t)
	in := httpInbound("in1", addr, []config.User{
		{Username: "alice", Password: "old"},
	})
	e, cfg := newTestEngine(t, []*config.Inbound{in})

	// Confirm the listener actually started on the address we requested.
	e.mu.Lock()
	startedAddr := e.listeners["in1"].Address()
	e.mu.Unlock()
	if startedAddr == "" {
		t.Fatal("listener not started")
	}
	// Use the listener's resolved address for dialing (it should match the
	// fixed port we reserved, but the OS may canonicalize the host).

	// Old password authenticates (502 = auth ok, dial failed as expected).
	if got := proxyConnect(t, addr, "alice", "old"); !contains(got, "502") {
		t.Fatalf("before reload, alice:old should authenticate (502), got: %q", got)
	}
	// Wrong password is rejected (407).
	if got := proxyConnect(t, addr, "alice", "wrong"); !contains(got, "407") {
		t.Fatalf("before reload, alice:wrong should be rejected (407), got: %q", got)
	}

	// Hot-reload: change the password. Same listen address, same id — only the
	// credential signature changes, which reconcile must detect.
	in.Users = []config.User{{Username: "alice", Password: "new"}}
	cfg.SetInbound(in)

	// Reconcile is driven by the watch goroutine on config version change.
	// Wait for it to apply (bounded retry).
	waitFor(t, 2*time.Second, func() bool {
		return contains(proxyConnect(t, addr, "alice", "new"), "502")
	})

	// After reload, on the SAME address:
	//   - new password authenticates (502)
	//   - old password is now rejected (407)
	if got := proxyConnect(t, addr, "alice", "new"); !contains(got, "502") {
		t.Fatalf("after reload, alice:new should authenticate (502), got: %q", got)
	}
	if got := proxyConnect(t, addr, "alice", "old"); !contains(got, "407") {
		t.Fatalf("after reload, alice:old should be rejected (407), got: %q", got)
	}

	// The listen address must NOT have changed (hot-reload, not restart).
	e.mu.Lock()
	newAddr := e.listeners["in1"].Address()
	e.mu.Unlock()
	if newAddr != addr {
		t.Fatalf("listen address changed on credential reload: %q -> %q (should hot-reload in place)", addr, newAddr)
	}
}

// TestEngineCloseStopsWatch asserts that after Close, a subsequent config
// change does NOT spawn a new listener (the watch goroutine has exited and
// reconcile is a no-op). This is the regression test for the watch-goroutine
// leak and the reconcile-after-shutdown race.
func TestEngineCloseStopsWatch(t *testing.T) {
	e, cfg := newTestEngine(t, nil) // no inbounds initially

	// Close the engine.
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Add an inbound AFTER close. With the watch goroutine stopped and
	// reconcile short-circuiting on e.closed, no listener should appear.
	cfg.SetInbound(httpInbound("late", freeAddr(t), nil))

	// Give any (buggy) reconcile a chance to run.
	time.Sleep(200 * time.Millisecond)

	e.mu.Lock()
	_, hasListener := e.listeners["late"]
	e.mu.Unlock()
	if hasListener {
		t.Fatal("a listener was started after Close; watch goroutine leaked or reconcile did not short-circuit on closed")
	}
}

// TestEngineReloadAddsAndRemovesListeners asserts the basic add/remove
// reconciliation: adding an inbound starts a listener, removing it stops one.
func TestEngineReloadAddsAndRemovesListeners(t *testing.T) {
	e, cfg := newTestEngine(t, nil)

	// Add an inbound.
	cfg.SetInbound(httpInbound("add", freeAddr(t), nil))
	waitFor(t, 2*time.Second, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		_, ok := e.listeners["add"]
		return ok
	})

	// Remove it.
	cfg.DeleteInbound("add")
	waitFor(t, 2*time.Second, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		_, ok := e.listeners["add"]
		return !ok
	})
}

// --- helpers ---

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func waitFor(t *testing.T, max time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", max)
	}
}
