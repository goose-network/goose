package socks5

import (
	"context"
	"io"
	"net"
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

// serveSocks5Echo is a minimal SOCKS5 server: it accepts the no-auth
// handshake, grants the CONNECT, and echoes everything after the tunnel is
// up, standing in for the remote proxy server.
func serveSocks5Echo(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			// greeting: VER NMETHODS METHODS...
			head := make([]byte, 2)
			if _, err := io.ReadFull(c, head); err != nil {
				return
			}
			methods := make([]byte, int(head[1]))
			if _, err := io.ReadFull(c, methods); err != nil {
				return
			}
			if _, err := c.Write([]byte{0x05, 0x00}); err != nil {
				return
			}
			// request: VER CMD RSV ATYP ADDR PORT
			req := make([]byte, 4)
			if _, err := io.ReadFull(c, req); err != nil {
				return
			}
			switch req[3] {
			case 0x01:
				if _, err := io.ReadFull(c, make([]byte, 4+2)); err != nil {
					return
				}
			case 0x03:
				l := make([]byte, 1)
				if _, err := io.ReadFull(c, l); err != nil {
					return
				}
				if _, err := io.ReadFull(c, make([]byte, int(l[0])+2)); err != nil {
					return
				}
			case 0x04:
				if _, err := io.ReadFull(c, make([]byte, 16+2)); err != nil {
					return
				}
			}
			// reply: success, bound addr 0.0.0.0:0
			reply := []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
			if _, err := c.Write(reply); err != nil {
				return
			}
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

// TestServerDialRidesUpstreamRoute: with an injected upstream-route dialer
// (UpstreamDialerKey), the outbound reaches its own proxy server through
// that route, and the SOCKS5 handshake + payload run over the tunnel.
func TestServerDialRidesUpstreamRoute(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go serveSocks5Echo(ln)

	route := &recordingDialer{}
	cfg := map[string]any{
		"id":                  "ob",
		"address":             ln.Addr().String(),
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

	if got := route.dialed(); len(got) != 1 || got[0] != ln.Addr().String() {
		t.Fatalf("server dial should ride the upstream route to %s, got %v", ln.Addr().String(), got)
	}
	// The SOCKS5 tunnel must be usable end to end.
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go serveSocks5Echo(ln)

	stk := &stackRecorder{}
	cfg := map[string]any{
		"id":               "ob",
		"address":          ln.Addr().String(),
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

	if got := stk.dialed(); len(got) != 1 || got[0] != ln.Addr().String() {
		t.Fatalf("server dial should ride the stack dialer to %s, got %v", ln.Addr().String(), got)
	}
}
