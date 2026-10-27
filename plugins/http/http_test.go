package http

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	pub "github.com/goose-network/goose/pkg/plugin"
)

// recordingDialer is a fake engine dialer: it records every dial target and
// then connects for real, so the outbound's server dial completes while
// proving it rode the injected dialer.
type recordingDialer struct {
	mu    sync.Mutex
	addrs []string
}

func (d *recordingDialer) DialUpstream(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, address)
	d.mu.Unlock()
	var nd net.Dialer
	return nd.DialContext(ctx, network, address)
}

func (d *recordingDialer) dialed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

// stackRecorder adapts the same recording behavior to the StackDialer shape.
type stackRecorder struct {
	recordingDialer
}

func (s *stackRecorder) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return s.DialUpstream(ctx, network, address)
}

// connectEcho serves HTTP CONNECT and echoes everything after the tunnel is
// up, standing in for the remote proxy server. The net/http server has
// already consumed the CONNECT request head before handing the connection
// over, so the handler only hijacks and answers.
type connectEcho struct{}

func (connectEcho) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	buf := make([]byte, 512)
	for {
		n, err := brw.Read(buf)
		if n > 0 {
			if _, werr := conn.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// TestServerDialRidesUpstreamRoute: with an injected upstream-route dialer
// (UpstreamDialerKey), the outbound reaches its own proxy server through
// that route, and the CONNECT handshake + payload run over the tunnel.
func TestServerDialRidesUpstreamRoute(t *testing.T) {
	srv := httptest.NewServer(connectEcho{})
	defer srv.Close()

	route := &recordingDialer{}
	cfg := map[string]any{
		"id":                  "ob",
		"address":             srv.Listener.Addr().String(),
		pub.UpstreamDialerKey: route,
	}
	out, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := out.DialContext(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial through upstream route: %v", err)
	}
	defer conn.Close()

	if got := route.dialed(); len(got) != 1 || got[0] != srv.Listener.Addr().String() {
		t.Fatalf("server dial should ride the upstream route to %s, got %v", srv.Listener.Addr().String(), got)
	}
	// The CONNECT tunnel must be usable end to end.
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo mismatch: %q", buf)
	}
}

// TestServerDialFallsBackToStack: without an upstream route, the server
// dial rides the injected engine stack dialer (StackDialerKey).
func TestServerDialFallsBackToStack(t *testing.T) {
	srv := httptest.NewServer(connectEcho{})
	defer srv.Close()

	stk := &stackRecorder{}
	cfg := map[string]any{
		"id":               "ob",
		"address":          srv.Listener.Addr().String(),
		pub.StackDialerKey: stk,
	}
	out, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := out.DialContext(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial through stack: %v", err)
	}
	defer conn.Close()

	if got := stk.dialed(); len(got) != 1 || got[0] != srv.Listener.Addr().String() {
		t.Fatalf("server dial should ride the stack dialer to %s, got %v", srv.Listener.Addr().String(), got)
	}
}
