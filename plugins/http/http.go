// Package http is the HTTP CONNECT outbound plugin. It dials a target by
// issuing an HTTP CONNECT to a configured upstream HTTP proxy server. It
// supports proxy chaining via the ChainDialer interface (dial this proxy's
// server through a previous connection).
package http

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"

	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/goose-network/goose/internal/core"
)

func init() {
	pub.Register("http", New)
}

// dialer is the engine-provided stack dialer (system or gvisor), injected
// via config under "_dialer" by the router. When absent we fall back to the
// standard dialer.
type dialer interface {
	Dial(ctx context.Context, network, address string) (net.Conn, error)
}

// Outbound dials targets through an upstream HTTP proxy (CONNECT).
type Outbound struct {
	id       string
	address  string // proxy server host:port
	username string
	password string
	location *core.Location
	dialer   dialer // engine stack dialer for reaching o.address
}

// New builds an HTTP outbound from config:
//
//	{"id":"...","address":"host:port","username":"...","password":"..."}
func New(cfg map[string]any) (core.Outbound, error) {
	addr, _ := cfg["address"].(string)
	if addr == "" {
		return nil, fmt.Errorf("http outbound: missing address")
	}
	o := &Outbound{
		id:       str(cfg, "id"),
		address:  addr,
		username: str(cfg, "username"),
		password: str(cfg, "password"),
	}
	if d, ok := cfg["_dialer"].(dialer); ok {
		o.dialer = d
	}
	return o, nil
}

func (o *Outbound) ID() string            { return o.id }
func (o *Outbound) Protocol() string      { return "http" }
func (o *Outbound) Address() string       { return o.address }
func (o *Outbound) Location() *core.Location { return o.location }
func (o *Outbound) Stats() core.OutboundStats { return core.OutboundStats{} }

func (o *Outbound) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	srv, err := o.dialServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("http outbound: dial server %s: %w", o.address, err)
	}
	return o.connect(srv, target)
}

// dialServer reaches this proxy's own server through the configured engine
// stack when one was injected, else the standard dialer.
func (o *Outbound) dialServer(ctx context.Context) (net.Conn, error) {
	if o.dialer != nil {
		return o.dialer.Dial(ctx, "tcp", o.address)
	}
	var nd net.Dialer
	return nd.DialContext(ctx, "tcp", o.address)
}

// DialThrough dials the target through this HTTP proxy, where the proxy's
// server is itself reached via a previous connection (chaining).
func (o *Outbound) DialThrough(ctx context.Context, network core.Network, target string, via net.Conn) (net.Conn, error) {
	return o.connect(via, target)
}

// connect issues HTTP CONNECT to target over srv and returns the tunneled conn.
func (o *Outbound) connect(srv net.Conn, target string) (net.Conn, error) {
	_ = srv.SetDeadline(time.Now().Add(30 * time.Second))
	req := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n"
	if o.username != "" {
		req += "Proxy-Authorization: Basic " + basicAuth(o.username, o.password) + "\r\n"
	}
	req += "\r\n"
	if _, err := srv.Write([]byte(req)); err != nil {
		srv.Close()
		return nil, fmt.Errorf("http outbound: write CONNECT: %w", err)
	}
	br := bufio.NewReader(srv)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		srv.Close()
		return nil, fmt.Errorf("http outbound: read CONNECT response: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		srv.Close()
		return nil, fmt.Errorf("http outbound: CONNECT %s failed: %s", target, resp.Status)
	}
	_ = srv.SetDeadline(time.Time{})
	if br.Buffered() > 0 {
		return &bufferedConn{br: br, Conn: srv}, nil
	}
	return srv, nil
}

func basicAuth(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}

type bufferedConn struct {
	br *bufio.Reader
	net.Conn
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	if b.br.Buffered() > 0 {
		return b.br.Read(p)
	}
	return b.Conn.Read(p)
}

func str(cfg map[string]any, key string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}
