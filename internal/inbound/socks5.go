package inbound

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// socks5Listener is a SOCKS5 (RFC 1928) proxy listener with username/password
// authentication (RFC 1929). It supports CONNECT for TCP. Each authenticated
// user resolves to its own policy via the PolicyResolver.
type socks5Listener struct {
	id      string
	addr    string
	auth    *AuthStore
	resolve PolicyResolver
	h       Handler
	ln      net.Listener
}

func newSOCKS5(id, listen string, auth *AuthStore, resolve PolicyResolver, h Handler) (*socks5Listener, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("inbound socks5: listen %s: %w", listen, err)
	}
	l := &socks5Listener{id: id, addr: ln.Addr().String(), auth: auth, resolve: resolve, h: h, ln: ln}
	go l.serve()
	return l, nil
}

func (l *socks5Listener) ID() string       { return l.id }
func (l *socks5Listener) Address() string  { return l.addr }
func (l *socks5Listener) Protocol() string { return "socks5" }
func (l *socks5Listener) Close() error     { return l.ln.Close() }

func (l *socks5Listener) serve() {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go l.handle(c)
	}
}

// SOCKS5 constants
const (
	socks5Ver  = 0x05
	methodNone = 0x00
	methodUser = 0x02
	methodNoneA = 0xFF // no acceptable methods
	cmdConnect = 0x01
	atypIPv4   = 0x01
	atypDomain = 0x03
	atypIPv6   = 0x04
	repSuccess = 0x00
	repFailure = 0x01
	repCmdNot  = 0x07
	repAtyp    = 0x08
)

func (l *socks5Listener) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Minute))

	// --- greeting ---
	ver, err := readByte(c)
	if err != nil || ver != socks5Ver {
		return
	}
	nmethods, err := readByte(c)
	if err != nil {
		return
	}
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(c, methods); err != nil {
		return
	}
	supportsNone := false
	supportsUser := false
	for _, m := range methods {
		switch m {
		case methodNone:
			supportsNone = true
		case methodUser:
			supportsUser = true
		}
	}
	var user string
	if l.auth.Enabled() {
		if !supportsUser {
			_, _ = c.Write([]byte{socks5Ver, methodNoneA})
			return
		}
		_, _ = c.Write([]byte{socks5Ver, methodUser})
		user, err = l.doUserPassAuth(c)
		if err != nil {
			return
		}
	} else {
		// No auth required. Per RFC 1928 we must select a method the client
		// offered. Prefer no-auth (0x00) when offered; if the client only
		// offered user/pass (0x02) we cannot select no-auth (the client
		// didn't offer it), so reply 0xFF (no acceptable methods) rather
		// than lying with 0x00.
		if supportsNone {
			_, _ = c.Write([]byte{socks5Ver, methodNone})
		} else {
			_, _ = c.Write([]byte{socks5Ver, methodNoneA})
			return
		}
	}

	// --- request ---
	if err := l.handleRequest(c, user); err != nil {
		// best-effort failure reply already sent inside
		_ = err
	}
}

func (l *socks5Listener) doUserPassAuth(c net.Conn) (string, error) {
	ver, err := readByte(c)
	if err != nil || ver != 0x01 {
		return "", err
	}
	ulen, err := readByte(c)
	if err != nil {
		return "", err
	}
	uname := make([]byte, ulen)
	if _, err := io.ReadFull(c, uname); err != nil {
		return "", err
	}
	plen, err := readByte(c)
	if err != nil {
		return "", err
	}
	pass := make([]byte, plen)
	if _, err := io.ReadFull(c, pass); err != nil {
		return "", err
	}
	if !l.auth.Verify(string(uname), string(pass)) {
		_, _ = c.Write([]byte{0x01, 0x01})
		return "", errors.New("socks5: auth failed")
	}
	_, _ = c.Write([]byte{0x01, 0x00})
	return string(uname), nil
}

func (l *socks5Listener) handleRequest(c net.Conn, user string) error {
	ver, err := readByte(c)
	if err != nil || ver != socks5Ver {
		return err
	}
	cmd, err := readByte(c)
	if err != nil {
		return err
	}
	if _, err := readByte(c); err != nil { // RSV
		return err
	}
	atyp, err := readByte(c)
	if err != nil {
		return err
	}
	host, err := readAddr(c, atyp)
	if err != nil {
		reply(c, repAtyp)
		return err
	}
	port, err := readPort(c)
	if err != nil {
		return err
	}
	if cmd != cmdConnect {
		reply(c, repCmdNot)
		return errors.New("socks5: only CONNECT supported")
	}
	meta := core.Metadata{
		InboundID:  l.id,
		User:       user,
		Network:    core.NetworkTCP,
		TargetHost: host,
		TargetPort: strconv.Itoa(int(port)),
		SourceIP:   remoteIP(c),
		StartedAt:  time.Now(),
	}
	// Clear the deadline; the router manages timeouts from here.
	_ = c.SetDeadline(time.Time{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	upstream, chain, err := l.h.Dial(ctx, meta)
	if err != nil {
		reply(c, repFailure)
		return err
	}
	meta.Chain = chain
	// Write the SOCKS5 success reply, then bridge.
	reply(c, repSuccess)
	l.h.Bridge(c, upstream, meta)
	return nil
}

// readAddr reads the address portion of a SOCKS5 request per atyp.
func readAddr(c net.Conn, atyp byte) (string, error) {
	switch atyp {
	case atypIPv4:
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	case atypIPv6:
		b := make([]byte, 16)
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	case atypDomain:
		ln, err := readByte(c)
		if err != nil {
			return "", err
		}
		b := make([]byte, ln)
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", errors.New("socks5: bad atyp")
	}
}

func readPort(c net.Conn) (uint16, error) {
	b := make([]byte, 2)
	if _, err := io.ReadFull(c, b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func readByte(c net.Conn) (byte, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(c, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

// reply writes a SOCKS5 reply with the given code and a zeroed bind address.
func reply(c net.Conn, code byte) {
	// VER, REP, RSV, ATYP=IPv4, 4 bytes addr, 2 bytes port
	_, _ = c.Write([]byte{socks5Ver, code, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0})
}
