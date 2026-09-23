// Package socks5 is the SOCKS5 outbound plugin. It dials a target by
// issuing a SOCKS5 CONNECT to a configured upstream SOCKS5 proxy server,
// with optional username/password authentication (RFC 1929). It supports
// proxy chaining via the ChainDialer interface.
package socks5

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/goose-network/goose/internal/core"
)

func init() {
	pub.Register("socks5", New)
}

// dialer is the engine-provided stack dialer (system or gvisor), injected
// via config under "_dialer" by the router. When absent we fall back to the
// standard dialer.
type dialer interface {
	Dial(ctx context.Context, network, address string) (net.Conn, error)
}

// Outbound dials targets through an upstream SOCKS5 proxy.
type Outbound struct {
	id       string
	address  string // proxy server host:port
	username string
	password string
	location *core.Location
	dialer   dialer // engine stack dialer for reaching o.address
}

// New builds a SOCKS5 outbound from config:
//
//	{"id":"...","address":"host:port","username":"...","password":"..."}
func New(cfg map[string]any) (core.Outbound, error) {
	addr, _ := cfg["address"].(string)
	if addr == "" {
		return nil, fmt.Errorf("socks5 outbound: missing address")
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
func (o *Outbound) Protocol() string      { return "socks5" }
func (o *Outbound) Address() string       { return o.address }
func (o *Outbound) Location() *core.Location { return o.location }
func (o *Outbound) Stats() core.OutboundStats { return core.OutboundStats{} }

func (o *Outbound) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	srv, err := o.dialServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("socks5 outbound: dial server %s: %w", o.address, err)
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

// DialThrough dials the target through this SOCKS5 proxy, where the proxy's
// server is reached via a previous connection (chaining).
func (o *Outbound) DialThrough(ctx context.Context, network core.Network, target string, via net.Conn) (net.Conn, error) {
	return o.connect(via, target)
}

func (o *Outbound) connect(srv net.Conn, target string) (net.Conn, error) {
	_ = srv.SetDeadline(time.Now().Add(30 * time.Second))
	defer func() { _ = srv.SetDeadline(time.Time{}) }()

	// greeting
	method := byte(0x00) // no auth
	if o.username != "" {
		method = 0x02 // user/pass
	}
	if _, err := srv.Write([]byte{0x05, 0x01, method}); err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: greeting: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(srv, resp); err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: greeting resp: %w", err)
	}
	if resp[0] != 0x05 {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: bad version %d", resp[0])
	}
	if resp[1] == 0xFF {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: no acceptable auth method")
	}
	if resp[1] == 0x02 {
		if err := o.userPass(srv); err != nil {
			srv.Close()
			return nil, err
		}
	}

	// CONNECT request
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: split target: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: bad port: %w", err)
	}
	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = append(req, 0x01)
			req = append(req, ip4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	req = append(req, 0, 0)
	binary.BigEndian.PutUint16(req[len(req)-2:], uint16(port))
	if _, err := srv.Write(req); err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: write CONNECT: %w", err)
	}
	// reply: VER, REP, RSV, ATYP, BND.ADDR, BND.PORT
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(srv, hdr); err != nil {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: read reply: %w", err)
	}
	if hdr[1] != 0x00 {
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: CONNECT failed code %d", hdr[1])
	}
	var skip int
	switch hdr[3] {
	case 0x01:
		skip = 4
	case 0x04:
		skip = 16
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(srv, l); err != nil {
			srv.Close()
			return nil, fmt.Errorf("socks5 outbound: read addr len: %w", err)
		}
		skip = int(l[0])
	}
	if skip > 0 {
		if _, err := io.ReadFull(srv, make([]byte, skip)); err != nil {
			srv.Close()
			return nil, fmt.Errorf("socks5 outbound: read bnd addr: %w", err)
		}
	}
	if _, err := io.ReadFull(srv, make([]byte, 2)); err != nil { // BND.PORT
		srv.Close()
		return nil, fmt.Errorf("socks5 outbound: read bnd port: %w", err)
	}
	return srv, nil
}

func (o *Outbound) userPass(srv net.Conn) error {
	uname := []byte(o.username)
	pass := []byte(o.password)
	req := []byte{0x01, byte(len(uname))}
	req = append(req, uname...)
	req = append(req, byte(len(pass)))
	req = append(req, pass...)
	if _, err := srv.Write(req); err != nil {
		return fmt.Errorf("socks5 outbound: write auth: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(srv, resp); err != nil {
		return fmt.Errorf("socks5 outbound: read auth resp: %w", err)
	}
	if resp[1] != 0x00 {
		return fmt.Errorf("socks5 outbound: auth failed")
	}
	return nil
}

func str(cfg map[string]any, key string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}
