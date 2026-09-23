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
