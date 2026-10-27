package direct

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	pub "github.com/goose-network/goose/pkg/plugin"
)

// recordingDialer is a fake engine dialer: it records every dial target and
// then connects for real, so the dial completes while proving it rode the
// injected dialer.
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

// echoListener accepts one connection and echoes everything on it.
func echoListener(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			buf := make([]byte, 512)
			for {
				n, err := c.Read(buf)
				if n > 0 {
					if _, werr := c.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}(conn)
	}
}

// TestDialRidesUpstreamRoute: with an injected upstream-route dialer
// (UpstreamDialerKey), the direct hop sends the target through that engine
// route instead of the local stack.
func TestDialRidesUpstreamRoute(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go echoListener(ln)

	route := &recordingDialer{}
	cfg := map[string]any{
		"id":                  "dir",
		pub.UpstreamDialerKey: route,
	}
	out, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := out.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial through upstream route: %v", err)
	}
	defer conn.Close()

	if got := route.dialed(); len(got) != 1 || got[0] != ln.Addr().String() {
		t.Fatalf("dial should ride the upstream route to %s, got %v", ln.Addr().String(), got)
	}
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

// TestDialFallsBackToStack: without an upstream route, the dial rides the
// injected engine stack dialer (StackDialerKey).
func TestDialFallsBackToStack(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go echoListener(ln)

	stk := &stackRecorder{}
	cfg := map[string]any{
		"id":               "dir",
		pub.StackDialerKey: stk,
	}
	out, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := out.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial through stack: %v", err)
	}
	defer conn.Close()

	if got := stk.dialed(); len(got) != 1 || got[0] != ln.Addr().String() {
		t.Fatalf("dial should ride the stack dialer to %s, got %v", ln.Addr().String(), got)
	}
}
