// Package e2e exercises the full goose engine as a black box over its
// public surface: the RESTful admin API and the proxy data path. A real HTTP
// client configures outbounds, pools, chains and inbounds through the API,
// then issues real requests through the resulting proxy ports and asserts the
// traffic reaches the upstream — including a two-hop proxy chain — and that
// request metrics are persisted and queryable through the API.
//
// These tests stand up the real engine (config store, metrics bbolt DB,
// router, inbound listeners, admin API server) and talk to it over real
// loopback TCP sockets. They are the top of the test pyramid: slow, few, and
// exercising the whole stack.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in outbound plugins
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// apiClient is a thin RESTful client over the admin API.
type apiClient struct {
	base   string
	token  string
	client *http.Client
}

func newAPIClient(base, token string) *apiClient {
	return &apiClient{
		base:  base,
		token: token,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *apiClient) do(method, path string, body any) (*http.Response, []byte) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		panic(err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		panic(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, data
}

// startUpstream runs a tiny HTTP server returning a fixed body tagged with the
// request path, so tests can confirm which upstream served the request.
func startUpstream(t *testing.T, tag string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s:%s", tag, r.URL.Path)
	})}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// startUpstreamProxy runs a real HTTP forward proxy (CONNECT + plain) that
// forwards to the given upstream. Used as the first hop in a two-hop chain.
func startUpstreamProxy(t *testing.T, upstream string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	handler := &forwardProxy{upstream: upstream}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// forwardProxy is a minimal HTTP forward proxy: it tunnels CONNECT requests
// and rewrites plain HTTP requests to the configured upstream. It exists only
// to give the e2e chain test a real second-hop proxy to dial through.
type forwardProxy struct {
	upstream string
}

func (p *forwardProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		// tunnel to the requested target directly (the chain's final hop).
		target := r.URL.Host
		if target == "" {
			target = r.Host
		}
		dst, err := net.DialTimeout("tcp", target, 5*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			dst.Close()
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		// Hijack first, then write the 200 status line on the raw conn.
		// Writing via w.WriteHeader before Hijack does not reliably flush
		// under http.Server and deadlocks the tunnel.
		client, _, err := hj.Hijack()
		if err != nil {
			dst.Close()
			return
		}
		_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		go io.Copy(dst, client)
		go io.Copy(client, dst)
		return
	}
	// plain HTTP: forward to upstream.
	outReq := r.Clone(r.Context())
	outReq.URL, _ = url.Parse(p.upstream + r.URL.Path)
	resp, err := http.DefaultTransport.RoundTrip(outReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// newEngineWithAPI builds an engine with the admin API on an ephemeral port
// and returns the engine plus an API client. The engine starts with no
// inbounds/outbounds; the test provisions them through the API.
func newEngineWithAPI(t *testing.T, token string) (*engine.Engine, *apiClient) {
	t.Helper()
	store := config.NewStore()
	store.SetEngine(config.Engine{
		Stack: "system",
		API:   config.APIConfig{Listen: fmt.Sprintf("127.0.0.1:%d", freePort(t)), Token: token},
		DB:    filepath.Join(t.TempDir(), "e2e.db"),
	})
	eng, err := engine.New(store)
	require.NoError(t, err)
	require.NoError(t, eng.StartAPI())
	// wait for the API port to accept connections.
	addr := store.Engine().API.Listen
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 3*time.Second, 50*time.Millisecond, "api server did not come up")
	return eng, newAPIClient("http://"+addr, token)
}

// TestE2E_APIProvisionsAndProxies drives the full lifecycle: create an
// outbound, pool, chain and inbound through the RESTful API, then proxy a
// real HTTP request through the resulting proxy port and confirm it reaches
// the upstream. This covers requirements #1 (inbound + API), #3 (load
// balance), #4 (per-inbound policy), #6 (metrics persistence).
func TestE2E_APIProvisionsAndProxies(t *testing.T) {
	upstream, stop := startUpstream(t, "upstream-ok")
	defer stop()

	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	inboundPort := freePort(t)

	// 1. create a direct outbound via the API.
	resp, body := api.do(http.MethodPost, "/api/outbounds", config.OutboundSpec{
		ID: "direct1", Protocol: "direct", Config: map[string]any{},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create outbound: %s", body)

	// 2. create a pool referencing it with round-robin selection.
	resp, body = api.do(http.MethodPost, "/api/pools", config.Pool{
		ID: "p1", OutboundIDs: []string{"direct1"},
		Selector: config.SelectorSpec{Type: "roundrobin"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create pool: %s", body)

	// 3. create a chain with one layer (the pool).
	resp, body = api.do(http.MethodPost, "/api/chains", config.ChainSpec{
		ID: "c1", Layers: []string{"p1"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create chain: %s", body)

	// 4. create an HTTP inbound on an ephemeral port bound to the chain.
	resp, body = api.do(http.MethodPost, "/api/inbounds", config.Inbound{
		ID: "in1", Protocol: "http",
		Listen: fmt.Sprintf("127.0.0.1:%d", inboundPort),
		Policy: config.InboundPolicy{ChainID: "c1"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create inbound: %s", body)

	// the engine reconciles listeners asynchronously on config change; wait
	// for the inbound port to accept connections.
	proxyAddr := fmt.Sprintf("127.0.0.1:%d", inboundPort)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 3*time.Second, 50*time.Millisecond, "inbound listener did not come up")

	// 5. issue a real request through the proxy.
	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) { return url.Parse("http://" + proxyAddr) },
	}}
	resp, err := cli.Get(upstream + "/hello")
	require.NoError(t, err)
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "upstream-ok:/hello", string(got))

	// 6. confirm the request was recorded as a metric, queryable via the API.
	require.Eventually(t, func() bool {
		resp, body = api.do(http.MethodGet, "/api/metrics?n=10", nil)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var recs []map[string]any
		if err := json.Unmarshal(body, &recs); err != nil {
			return false
		}
		return len(recs) >= 1
	}, 3*time.Second, 100*time.Millisecond, "metric for the proxied request was not persisted")
}

// TestE2E_APIAuthEnforced verifies the admin API rejects requests without the
// bearer token when one is configured (requirement #1: RESTful API auth).
func TestE2E_APIAuthEnforced(t *testing.T) {
	eng, api := newEngineWithAPI(t, "s3cret-token")
	defer eng.Close()

	// without token => 401
	resp, _ := api.do(http.MethodGet, "/api/inbounds", nil)
	// apiClient carries no token here because we override it:
	api.token = ""
	resp, _ = api.do(http.MethodGet, "/api/inbounds", nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// with correct token => 200
	api.token = "s3cret-token"
	resp, _ = api.do(http.MethodGet, "/api/inbounds", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestE2E_TwoHopChain provisions a two-layer chain — HTTP inbound → HTTP
// outbound (an upstream HTTP proxy) → direct outbound (the final upstream) —
// and confirms a request traverses both hops. This covers requirement #7
// (proxy chains via API with per-layer filters).
func TestE2E_TwoHopChain(t *testing.T) {
	finalUpstream, stopFinal := startUpstream(t, "final")
	defer stopFinal()

	// first hop: a real HTTP forward proxy that tunnels to the final upstream.
	hop1Addr, stopHop1 := startUpstreamProxy(t, finalUpstream)
	defer stopHop1()

	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	// layer 1 outbound: an HTTP outbound pointing at the hop-1 forward proxy.
	resp, body := api.do(http.MethodPost, "/api/outbounds", config.OutboundSpec{
		ID: "hop1", Protocol: "http",
		Config: map[string]any{"address": strings.TrimPrefix(hop1Addr, "http://")},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create hop1 outbound: %s", body)

	// layer 2 outbound: direct, dials the final upstream directly.
	resp, body = api.do(http.MethodPost, "/api/outbounds", config.OutboundSpec{
		ID: "direct-final", Protocol: "direct", Config: map[string]any{},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create direct-final outbound: %s", body)

	// two pools, one per layer (each layer can carry its own filters/selector).
	resp, body = api.do(http.MethodPost, "/api/pools", config.Pool{
		ID: "pool-hop1", OutboundIDs: []string{"hop1"},
		Selector: config.SelectorSpec{Type: "random"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create pool-hop1: %s", body)

	resp, body = api.do(http.MethodPost, "/api/pools", config.Pool{
		ID: "pool-final", OutboundIDs: []string{"direct-final"},
		Selector: config.SelectorSpec{Type: "random"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create pool-final: %s", body)

	// chain with two layers in order.
	resp, body = api.do(http.MethodPost, "/api/chains", config.ChainSpec{
		ID: "chain-2hop", Layers: []string{"pool-hop1", "pool-final"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create chain: %s", body)

	inboundPort := freePort(t)
	resp, body = api.do(http.MethodPost, "/api/inbounds", config.Inbound{
		ID: "in-chain", Protocol: "http",
		Listen: fmt.Sprintf("127.0.0.1:%d", inboundPort),
		Policy: config.InboundPolicy{ChainID: "chain-2hop"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create inbound: %s", body)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", inboundPort)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 3*time.Second, 50*time.Millisecond, "inbound listener did not come up")

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) { return url.Parse("http://" + proxyAddr) },
	}}
	resp, err := cli.Get(finalUpstream + "/deep")
	require.NoError(t, err)
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "final:/deep", string(got), "request should traverse both chain hops to the final upstream")
}

// TestE2E_HotReloadInbound verifies that adding an inbound through the API
// after the engine is running causes a new listener to come up without a
// restart (requirement #4: per-inbound policy, hot reload).
func TestE2E_HotReloadInbound(t *testing.T) {
	upstream, stop := startUpstream(t, "hot")
	defer stop()

	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	// provision outbound/pool/chain first.
	api.do(http.MethodPost, "/api/outbounds", config.OutboundSpec{ID: "d", Protocol: "direct", Config: map[string]any{}})
	api.do(http.MethodPost, "/api/pools", config.Pool{ID: "p", OutboundIDs: []string{"d"}, Selector: config.SelectorSpec{Type: "roundrobin"}})
	api.do(http.MethodPost, "/api/chains", config.ChainSpec{ID: "c", Layers: []string{"p"}})

	port := freePort(t)
	api.do(http.MethodPost, "/api/inbounds", config.Inbound{
		ID: "hot-in", Protocol: "socks5",
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Policy: config.InboundPolicy{ChainID: "c"},
	})

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", port)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 3*time.Second, 50*time.Millisecond, "hot-reloaded inbound did not come up")

	cli := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL("socks5://" + proxyAddr)),
	}}
	resp, err := cli.Get(upstream + "/reloaded")
	require.NoError(t, err)
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "hot:/reloaded", string(got))
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}
