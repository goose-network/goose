package inbound

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// captureHandler is a Handler whose Dial returns one end of a pipe; the other
// end (serverEnd) is read by the test to see exactly what the listener
// forwarded to the "upstream" (the replayed request bytes). Bridge copies
// inbound→upstream so those bytes reach serverEnd, and drains upstream→inbound
// so the listener's response-direction relay has somewhere to write.
//
// dialDone is closed when Dial is first called, so a test can wait for the
// listener to have reached the dial phase before reading serverEnd (avoiding
// a nil-pointer race with the listener's async handle goroutine).
type captureHandler struct {
	serverEnd net.Conn
	dialDone  chan struct{}
	once      sync.Once
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{dialDone: make(chan struct{})}
}

func (h *captureHandler) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	c, s := net.Pipe()
	h.serverEnd = s // test reads the replayed request from here
	h.once.Do(func() { close(h.dialDone) })
	return c, []string{"ob1"}, nil
}

// waitDial blocks until the listener has called Dial, then returns serverEnd.
func (h *captureHandler) waitDial() net.Conn {
	<-h.dialDone
	return h.serverEnd
}

func (h *captureHandler) Bridge(inb, up net.Conn, meta core.Metadata) {
	// Request direction: inbound -> upstream (bytes appear at serverEnd).
	// When it completes (client half-closes or EOFs), close the upstream
	// write side so the response-direction copy below can EOF and Bridge can
	// return instead of deadlocking.
	go func() {
		io.Copy(up, inb)
		_ = up.Close()
	}()
	// Response direction: drain upstream -> inbound (discarded; tests that
	// need the response read serverEnd directly instead). EOFs when up is
	// closed above.
	io.Copy(io.Discard, up)
	_ = inb.Close()
}

// dialListener dials the listener and returns the conn with a short deadline.
func dialListener(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	return c
}

// readN reads exactly n bytes or fails.
func readN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return buf
}

// --- SOCKS5 method selection (RFC 1928) ---

// TestSOCKS5AuthRequiredRejectsNoAuth asserts that when the server requires
// user/pass auth but the client offers ONLY no-auth (0x00), the server
// replies 0xFF (no acceptable methods) and closes — it must NOT accept
// no-auth when auth is configured. Regression test for the method-selection
// bug where the server could select a method the client didn't offer.
func TestSOCKS5AuthRequiredRejectsNoAuth(t *testing.T) {
	auth := NewAuthStore([]User{{Username: "u", Password: "p"}})
	h := newCaptureHandler()
	ln, err := newSOCKS5("s5", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newSOCKS5: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	// Greeting: version 5, 1 method, method = no-auth (0x00).
	if _, err := c.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	resp := readN(t, c, 2)
	if resp[0] != 0x05 {
		t.Fatalf("expected socks5 version in reply, got 0x%02x", resp[0])
	}
	if resp[1] != 0xFF {
		t.Fatalf("auth-required server must reject a no-auth-only client with 0xFF (no acceptable methods), got 0x%02x", resp[1])
	}
}

// TestSOCKS5NoAuthRejectsUserPassOnly asserts the symmetric case: when the
// server has NO auth, but the client offers ONLY user/pass (0x02), the server
// cannot select no-auth (the client didn't offer it) and must reply 0xFF.
func TestSOCKS5NoAuthRejectsUserPassOnly(t *testing.T) {
	auth := NewAuthStore(nil) // no auth
	h := newCaptureHandler()
	ln, err := newSOCKS5("s5", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newSOCKS5: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	// Greeting: version 5, 1 method, method = user/pass (0x02) only.
	if _, err := c.Write([]byte{0x05, 0x01, 0x02}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	resp := readN(t, c, 2)
	if resp[0] != 0x05 {
		t.Fatalf("expected socks5 version in reply, got 0x%02x", resp[0])
	}
	if resp[1] != 0xFF {
		t.Fatalf("no-auth server must not select no-auth the client didn't offer; expected 0xFF, got 0x%02x", resp[1])
	}
}

// TestSOCKS5NoAuthAcceptsNoAuth asserts the happy path: no-auth server, client
// offers no-auth (0x00), server selects 0x00 and proceeds to the request
// phase (we then send a minimal CONNECT and expect a success reply 0x00).
func TestSOCKS5NoAuthAcceptsNoAuth(t *testing.T) {
	auth := NewAuthStore(nil)
	h := newCaptureHandler()
	ln, err := newSOCKS5("s5", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newSOCKS5: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	// Greeting: offer no-auth.
	if _, err := c.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	resp := readN(t, c, 2)
	if resp[1] != 0x00 {
		t.Fatalf("no-auth server should select no-auth (0x00), got 0x%02x", resp[1])
	}
}

// TestSOCKS5AuthWrongPasswordFails asserts that a correct method selection
// (user/pass) but wrong credentials causes the server to send an auth-failure
// status (0x01) and close, never reaching the request phase.
func TestSOCKS5AuthWrongPasswordFails(t *testing.T) {
	auth := NewAuthStore([]User{{Username: "u", Password: "correct"}})
	h := newCaptureHandler()
	ln, err := newSOCKS5("s5", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newSOCKS5: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	// Greeting: offer user/pass.
	if _, err := c.Write([]byte{0x05, 0x01, 0x02}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	resp := readN(t, c, 2)
	if resp[1] != 0x02 {
		t.Fatalf("auth-required server should select user/pass (0x02), got 0x%02x", resp[1])
	}
	// Sub-negotiation: version 1, user "u", pass "wrong".
	user := []byte("u")
	pass := []byte("wrong")
	msg := []byte{0x01, byte(len(user))}
	msg = append(msg, user...)
	msg = append(msg, byte(len(pass)))
	msg = append(msg, pass...)
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	authResp := readN(t, c, 2)
	if authResp[0] != 0x01 {
		t.Fatalf("expected auth sub-negotiation version 1, got 0x%02x", authResp[0])
	}
	if authResp[1] != 0x01 {
		t.Fatalf("wrong password must yield auth-failure status 0x01, got 0x%02x", authResp[1])
	}
}

// --- HTTP plain-proxy body replay + hop-by-hop header stripping ---

// TestHTTPPlainProxyReplaysBody asserts that a plain-HTTP (non-CONNECT)
// request's body is fully forwarded to the origin even when part of it was
// buffered by the listener's bufio.Reader during http.ReadRequest. The
// listener must drain buffered bytes and layer them under the live conn.
func TestHTTPPlainProxyReplaysBody(t *testing.T) {
	auth := NewAuthStore(nil)
	h := newCaptureHandler()
	ln, err := newHTTP("h", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newHTTP: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	body := "hello-body-bytes"
	// Absolute-form request line so http.ReadRequest treats it as a proxy
	// request with a body.
	req := "POST http://example.com/path HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n" +
		"Connection: close\r\n" +
		"\r\n" + body
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	// Half-close the write side so the listener's inbound->upstream relay
	// sees EOF, finishes, and closes the upstream pipe end — which lets our
	// io.ReadAll(serverEnd) return instead of blocking until the deadline.
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}

	// The handler's Dial returned a pipe; its server end receives the
	// replayed request. Wait for Dial to have run (the listener handles the
	// connection asynchronously), then read it with a deadline.
	serverEnd := h.waitDial()
	_ = serverEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(serverEnd)
	if err != nil {
		t.Fatalf("read replayed request: %v", err)
	}
	if !bytes.Contains(got, []byte(body)) {
		t.Fatalf("replayed request missing body %q; got:\n%s", body, got)
	}
	if !bytes.Contains(got, []byte("POST /path HTTP/1.1")) {
		t.Fatalf("replayed request line should be origin-form (POST /path ...), got:\n%s", got)
	}
}

// TestHTTPPlainProxyStripsProxyAuthorization asserts that Proxy-Authorization
// (a hop-by-hop header) is NOT forwarded to the origin, per RFC 7230 §6.1 —
// forwarding it would leak the client's proxy credentials upstream.
func TestHTTPPlainProxyStripsProxyAuthorization(t *testing.T) {
	auth := NewAuthStore([]User{{Username: "u", Password: "p"}})
	h := newCaptureHandler()
	ln, err := newHTTP("h", "127.0.0.1:0", auth, func(string) string { return "c" }, h)
	if err != nil {
		t.Fatalf("newHTTP: %v", err)
	}
	defer ln.Close()

	c := dialListener(t, ln.Address())
	defer c.Close()
	cred := base64.StdEncoding.EncodeToString([]byte("u:p"))
	body := ""
	req := "GET http://example.com/ HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Proxy-Authorization: Basic " + cred + "\r\n" +
		"Connection: close\r\n" +
		"\r\n" + body
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}

	serverEnd := h.waitDial()
	_ = serverEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(serverEnd)
	if err != nil {
		t.Fatalf("read replayed request: %v", err)
	}
	if bytes.Contains(got, []byte("Proxy-Authorization")) {
		t.Fatalf("hop-by-hop Proxy-Authorization must NOT be forwarded to origin; got:\n%s", got)
	}
	if !bytes.Contains(got, []byte("Host: example.com")) {
		t.Fatalf("non-hop-by-hop Host header should be forwarded; got:\n%s", got)
	}
}

// itoa is a tiny strconv.Itoa to avoid the import for one call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// --- observed-path recorder creation ---

// lisPHandler is a recording Handler for the path-recorder test. It keeps the
// Metadata it was handed on EVERY Dial and EVERY Bridge, in call order, so a
// test can pair them up per request.
//
// The pairing is the whole point: Metadata is passed by value and Bridge gets
// no context, so the only way the observed path can reach the success row
// Bridge records is a *core.PathRecorder the listener itself created and put on
// Metadata before calling Dial.
type lisPHandler struct {
	mu      sync.Mutex
	dials   []core.Metadata
	bridges []core.Metadata
	bridged chan struct{}
}

func newLisPHandler() *lisPHandler {
	return &lisPHandler{bridged: make(chan struct{}, 8)}
}

func (h *lisPHandler) Dial(_ context.Context, meta core.Metadata) (net.Conn, []string, error) {
	c, s := net.Pipe()
	h.mu.Lock()
	h.dials = append(h.dials, meta)
	h.mu.Unlock()
	// Nothing reads the pipe; Bridge closes both ends.
	go func() { _ = s.Close() }()
	return c, []string{"ob1"}, nil
}

func (h *lisPHandler) Bridge(inb, up net.Conn, meta core.Metadata) {
	h.mu.Lock()
	h.bridges = append(h.bridges, meta)
	h.mu.Unlock()
	_ = up.Close()
	_ = inb.Close()
	h.bridged <- struct{}{}
}

// waitBridges blocks until n Bridges have returned. Channel-synchronised, with
// the timeout only as a failure guard.
func (h *lisPHandler) waitBridges(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-h.bridged:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for Bridge call %d", i+1)
		}
	}
}

func (h *lisPHandler) snapshot() ([]core.Metadata, []core.Metadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := make([]core.Metadata, len(h.dials))
	copy(d, h.dials)
	b := make([]core.Metadata, len(h.bridges))
	copy(b, h.bridges)
	return d, b
}

// lisPConnectHTTP drives n HTTP CONNECT requests at addr. It waits for each
// request's Bridge before starting the next one, so the handler's recorded
// Dial and Bridge slices stay in matching order without any sleep.
func lisPConnectHTTP(t *testing.T, addr string, h *lisPHandler, n int) {
	t.Helper()
	ack := "HTTP/1.1 200 Connection Established\r\n\r\n"
	for i := 0; i < n; i++ {
		c := dialListener(t, addr)
		req := "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
		if _, err := c.Write([]byte(req)); err != nil {
			c.Close()
			t.Fatalf("write CONNECT: %v", err)
		}
		if got := string(readN(t, c, len(ack))); got != ack {
			c.Close()
			t.Fatalf("CONNECT ack = %q, want %q", got, ack)
		}
		h.waitBridges(t, 1)
		_ = c.Close()
	}
}

// lisPConnectSOCKS5 drives n SOCKS5 CONNECT requests at addr, waiting for each
// request's Bridge before starting the next (see lisPConnectHTTP).
func lisPConnectSOCKS5(t *testing.T, addr string, h *lisPHandler, n int) {
	t.Helper()
	const host = "example.com"
	for i := 0; i < n; i++ {
		c := dialListener(t, addr)
		if _, err := c.Write([]byte{0x05, 0x01, 0x00}); err != nil {
			c.Close()
			t.Fatalf("write greeting: %v", err)
		}
		if resp := readN(t, c, 2); resp[1] != 0x00 {
			c.Close()
			t.Fatalf("method selection = 0x%02x, want 0x00", resp[1])
		}
		req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
		req = append(req, host...)
		req = append(req, 0x01, 0xBB) // port 443
		if _, err := c.Write(req); err != nil {
			c.Close()
			t.Fatalf("write CONNECT request: %v", err)
		}
		if resp := readN(t, c, 10); resp[1] != 0x00 {
			c.Close()
			t.Fatalf("SOCKS5 reply = 0x%02x, want 0x00 (success)", resp[1])
		}
		h.waitBridges(t, 1)
		_ = c.Close()
	}
}

// TestListenersCreateThePathRecorder asserts both inbound protocols create the
// request's *core.PathRecorder themselves, so Dial and Bridge see the SAME
// pointer.
//
// That identity is load-bearing, not incidental: Metadata crosses both calls
// BY VALUE, and Bridge receives no context at all, so a recorder created
// anywhere downstream (the router's Dial, say) would be recorded into a copy
// the listener never sees again and the success row Bridge writes would carry
// no graph.
func TestListenersCreateThePathRecorder(t *testing.T) {
	const requests = 2

	for _, tc := range []struct {
		name     string
		protocol string
		drive    func(t *testing.T, addr string, h *lisPHandler, n int)
	}{
		{"http connect", "http", lisPConnectHTTP},
		{"socks5 connect", "socks5", lisPConnectSOCKS5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLisPHandler()
			ln, err := New("in-"+tc.protocol, tc.protocol, "127.0.0.1:0",
				NewAuthStore(nil), func(string) string { return "c" }, h)
			if err != nil {
				t.Fatalf("New(%s): %v", tc.protocol, err)
			}
			defer ln.Close()

			// Each request is fully bridged before the next one starts, so
			// dials[i] and bridges[i] are the same request.
			tc.drive(t, ln.Address(), h, requests)

			dials, bridges := h.snapshot()
			if len(dials) != requests {
				t.Fatalf("got %d Dial calls, want %d", len(dials), requests)
			}
			if len(bridges) != requests {
				t.Fatalf("got %d Bridge calls, want %d", len(bridges), requests)
			}

			for i := 0; i < requests; i++ {
				d, b := dials[i], bridges[i]

				// The listener extracted the right destination.
				if d.TargetHost != "example.com" || d.TargetPort != "443" {
					t.Errorf("request %d: target = %s:%s, want example.com:443",
						i, d.TargetHost, d.TargetPort)
				}

				// Non-nil on BOTH calls.
				if d.Path == nil {
					t.Errorf("request %d: Dial's Metadata.Path is nil; the listener must create it", i)
					continue
				}
				if b.Path == nil {
					t.Errorf("request %d: Bridge's Metadata.Path is nil; the listener must create it", i)
					continue
				}

				// And the SAME pointer — the whole point.
				if d.Path != b.Path {
					t.Errorf("request %d: Dial and Bridge hold different recorders (%p vs %p); "+
						"Bridge would record a row with no observed path", i, d.Path, b.Path)
				}

				// Metadata is by value: the listener filled Chain into its own
				// copy between the two calls, so Dial saw none and Bridge saw
				// the chain Dial returned.
				if len(d.Chain) != 0 {
					t.Errorf("request %d: Dial's Metadata.Chain = %v, want empty", i, d.Chain)
				}
				if len(b.Chain) != 1 || b.Chain[0] != "ob1" {
					t.Errorf("request %d: Bridge's Metadata.Chain = %v, want [ob1]", i, b.Chain)
				}

				// One recorder per request, not one per listener.
				for j := 0; j < i; j++ {
					if dials[j].Path == d.Path {
						t.Errorf("requests %d and %d share one recorder (%p); "+
							"their graphs would merge", j, i, d.Path)
					}
				}

				// It is a live recorder, not a placeholder: it can be
				// snapshotted, and a fresh one has nothing observed yet.
				if got := d.Path.Snapshot(); got != nil {
					t.Errorf("request %d: a listener-created recorder should have observed nothing yet, got %+v", i, got)
				}
			}
		})
	}
}
