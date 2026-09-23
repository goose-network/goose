package inbound

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// httpListener is an HTTP proxy listener. It supports:
//   - CONNECT method (tunneling for HTTPS / arbitrary TCP)
//   - plain HTTP proxying (GET/POST/... forwarded to the origin)
//   - Proxy-Authorization: Basic, with per-user policy dispatch
//
// Authenticated/authorized connections are handed to the Handler, which
// dials the target through the engine and bridges the streams.
type httpListener struct {
	id      string
	addr    string
	auth    *AuthStore
	resolve PolicyResolver
	h       Handler
	ln      net.Listener
}

func newHTTP(id, listen string, auth *AuthStore, resolve PolicyResolver, h Handler) (*httpListener, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("inbound http: listen %s: %w", listen, err)
	}
	l := &httpListener{id: id, addr: ln.Addr().String(), auth: auth, resolve: resolve, h: h, ln: ln}
	go l.serve()
	return l, nil
}

func (l *httpListener) ID() string       { return l.id }
func (l *httpListener) Address() string  { return l.addr }
func (l *httpListener) Protocol() string { return "http" }
func (l *httpListener) Close() error     { return l.ln.Close() }

func (l *httpListener) serve() {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go l.handle(c)
	}
}

func (l *httpListener) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Minute))
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	user := l.authenticate(req)
	if l.auth.Enabled() && user == "" {
		_, _ = fmt.Fprintf(c, "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"goose\"\r\nContent-Length: 0\r\n\r\n")
		return
	}
	host := req.URL.Host
	if host == "" {
		host = req.Host
	}
	if host == "" {
		_, _ = fmt.Fprintf(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
		return
	}
	h, p, err := net.SplitHostPort(host)
	if err != nil {
		h, p = host, "80"
	}
	meta := core.Metadata{
		InboundID:  l.id,
		User:       user,
		Network:    core.NetworkTCP,
		TargetHost: h,
		TargetPort: p,
		SourceIP:   remoteIP(c),
		StartedAt:  time.Now(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if req.Method == http.MethodConnect {
		upstream, chain, err := l.h.Dial(ctx, meta)
		if err != nil {
			_, _ = fmt.Fprintf(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
			return
		}
		meta.Chain = chain
		// Acknowledge the tunnel, then bridge raw bytes.
		_, _ = fmt.Fprintf(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
		var rc net.Conn = c
		if br.Buffered() > 0 {
			rc = &bufferedConn{br: br, Conn: c}
		}
		l.h.Bridge(rc, upstream, meta)
		return
	}

	// Plain HTTP: dial the origin, then replay the serialized request to it
	// and bridge the response back. The request body (if any) is read from
	// the live connection after the prefix.
	upstream, chain, err := l.h.Dial(ctx, meta)
	if err != nil {
		_, _ = fmt.Fprintf(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		return
	}
	meta.Chain = chain
	ser := serializeRequest(req)
	// The request body (if any) may have been partially buffered by the
	// bufio.Reader during http.ReadRequest. Drain those buffered bytes first
	// (like the CONNECT path does), then layer the serialized request prefix
	// on top so the origin receives the full request line + headers + body.
	var rc net.Conn = c
	if br.Buffered() > 0 {
		rc = &bufferedConn{br: br, Conn: c}
	}
	rc = &replayConn{prefix: []byte(ser), Conn: rc}
	l.h.Bridge(rc, upstream, meta)
}

// authenticate extracts and validates Proxy-Authorization, returning the
// username (or "" if no auth / disabled / failed).
func (l *httpListener) authenticate(req *http.Request) string {
	if !l.auth.Enabled() {
		return ""
	}
	hdr := req.Header.Get("Proxy-Authorization")
	if !strings.HasPrefix(hdr, "Basic ") {
		return ""
	}
	dec, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(hdr, "Basic "))
	if err != nil {
		return ""
	}
	pair := strings.SplitN(string(dec), ":", 2)
	if len(pair) != 2 || !l.auth.Verify(pair[0], pair[1]) {
		return ""
	}
	return pair[0]
}

// serializeRequest reconstructs the wire bytes of a request for replay. It
// forces "Connection: close" so the origin closes after sending the response,
// which lets the generic byte-bridge detect end-of-response and record the
// request metric promptly (otherwise HTTP/1.1 keep-alive would hold the
// bridge open indefinitely).
func serializeRequest(req *http.Request) string {
	var b strings.Builder
	b.WriteString(req.Method)
	b.WriteString(" ")
	if req.URL.RequestURI() == "" {
		b.WriteString("/")
	} else {
		b.WriteString(req.URL.RequestURI())
	}
	b.WriteString(" HTTP/1.1\r\nHost: ")
	b.WriteString(req.Host)
	b.WriteString("\r\n")
	// Strip hop-by-hop headers before replaying to the origin. These are
	// meaningful only for the immediate proxy hop (the client->goose link)
	// and must not be forwarded: Proxy-Authorization would leak the client's
	// proxy credentials to the origin server. Per RFC 7230 §6.1.
	for _, h := range hopByHopHeaders {
		req.Header.Del(h)
	}
	// Drop any client-supplied Connection header and force close.
	req.Header.Del("Connection")
	req.Header.Set("Connection", "close")
	req.Header.Write(&b)
	b.WriteString("\r\n")
	return b.String()
}

// hopByHopHeaders are per-connection headers that must not be forwarded by a
// proxy (RFC 7230 §6.1). Connection is handled separately because we force it
// to "close" for prompt metric recording (see serializeRequest).
var hopByHopHeaders = []string{
	"Proxy-Authorization",
	"Proxy-Connection",
	"Keep-Alive",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// bufferedConn lets the router read bytes already buffered by the
// bufio.Reader before falling through to the raw connection.
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

// replayConn serves a fixed prefix of bytes (the serialized request) then
// the underlying connection for any request body.
type replayConn struct {
	prefix []byte
	net.Conn
}

func (r *replayConn) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	return r.Conn.Read(p)
}

func remoteIP(c net.Conn) net.IP {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP
	}
	return nil
}
