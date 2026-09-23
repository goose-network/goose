// Package integration tests the engine end-to-end at the loopback level:
// a real upstream HTTP server, a configured direct outbound + pool + chain,
// and the HTTP/SOCKS5 inbound listeners. A real HTTP client issues a request
// through the inbound proxy and we assert it reaches the upstream.
package integration

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in outbound plugins
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/engine"
	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startUpstream runs a tiny HTTP server returning a fixed body, returns its
// base URL and a shutdown func.
func startUpstream(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "upstream-ok:%s", r.URL.Path)
	})}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// newEngine builds an engine with a direct outbound, a pool, a chain, and an
// inbound of the given protocol on an ephemeral port. Returns the engine and
// the inbound's listen address.
func newEngine(t *testing.T, protocol, user, pass string) (*engine.Engine, string) {
	t.Helper()
	store := config.NewStore()
	store.SetEngine(config.Engine{Stack: "system", API: config.APIConfig{Listen: "127.0.0.1:0"}, DB: filepath.Join(t.TempDir(), "it.db")})

	// direct outbound
	store.SetOutbound(&config.OutboundSpec{ID: "direct1", Protocol: "direct", Config: map[string]any{}})
	// pool -> chain
	store.SetPool(&config.Pool{ID: "p1", OutboundIDs: []string{"direct1"}, Selector: config.SelectorSpec{Type: "roundrobin"}})
	store.SetChain(&config.ChainSpec{ID: "c1", Layers: []string{"p1"}})

	// inbound
	inboundPort := freePort(t)
	var users []config.User
	if user != "" {
		users = []config.User{{Username: user, Password: pass}}
	}
	store.SetInbound(&config.Inbound{
		ID:       "in1",
		Protocol: protocol,
		Listen:   fmt.Sprintf("127.0.0.1:%d", inboundPort),
		Users:    users,
		Policy:   config.InboundPolicy{ChainID: "c1"},
	})

	eng, err := engine.New(store)
	require.NoError(t, err)
	return eng, fmt.Sprintf("127.0.0.1:%d", inboundPort)
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func TestDirectOutboundRegistered(t *testing.T) {
	// the direct plugin must be registered via the include import pulled in
	// by the engine's transitive deps. Confirm it's available.
	_, ok := pub.Lookup("direct")
	assert.True(t, ok, "direct plugin should be registered")
}

func TestHTTPInboundProxiesToUpstream(t *testing.T) {
	upstream, stop := startUpstream(t)
	defer stop()
	eng, addr := newEngine(t, "http", "", "")
	defer eng.Close()

	// HTTP client through the proxy.
	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse("http://" + addr)
		},
	}}
	resp, err := cli.Get(upstream + "/hello")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "upstream-ok:/hello", string(body))
}

func TestHTTPInboundWithAuth(t *testing.T) {
	upstream, stop := startUpstream(t)
	defer stop()
	eng, addr := newEngine(t, "http", "alice", "secret")
	defer eng.Close()

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL("http://alice:secret@" + addr)),
	}}
	resp, err := cli.Get(upstream + "/authed")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "upstream-ok:/authed", string(body))
}

func TestHTTPInboundAuthRejected(t *testing.T) {
	upstream, stop := startUpstream(t)
	defer stop()
	eng, addr := newEngine(t, "http", "alice", "secret")
	defer eng.Close()

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL("http://bob:wrong@" + addr)),
	}}
	resp, err := cli.Get(upstream + "/nope")
	if err == nil {
		defer resp.Body.Close()
	}
	// wrong creds => 407 or transport error
	if resp != nil {
		assert.Equal(t, http.StatusProxyAuthRequired, resp.StatusCode)
	}
}

func TestSOCKS5InboundProxiesToUpstream(t *testing.T) {
	upstream, stop := startUpstream(t)
	defer stop()
	eng, addr := newEngine(t, "socks5", "", "")
	defer eng.Close()

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL("socks5://" + addr)),
	}}
	resp, err := cli.Get(upstream + "/socks")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "upstream-ok:/socks", string(body))
}

func TestSOCKS5InboundWithAuth(t *testing.T) {
	upstream, stop := startUpstream(t)
	defer stop()
	eng, addr := newEngine(t, "socks5", "alice", "secret")
	defer eng.Close()

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL("socks5://alice:secret@" + addr)),
	}}
	resp, err := cli.Get(upstream + "/socksauth")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "upstream-ok:/socksauth", string(body))
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

// keep base64 import used (for future auth header tests)
var _ = base64.StdEncoding
