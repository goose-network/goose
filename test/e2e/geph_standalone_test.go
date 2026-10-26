// Standalone end-to-end test for the "geph" provider plugin inside the goose
// engine: NO Rust geph5-exit, no env vars, no second process. The provider's
// "embedded_exit" option boots the geph5 exit (the Go port in
// geph5-go/internal/exitlib) inside the engine's own process, so the ENTIRE
// chain — engine, admin API, geph client, geph exit, and the target HTTP
// server — lives in the test process:
//
//	HTTP client → goose http inbound → pool policy → socks5 outbound →
//	  embedded geph5 client (plugin) → encrypted tunnel → embedded geph5
//	  exit → loopback target
//
// This runs in the default suite (it is the continuously-verified proof that
// goose + the geph plugin work standalone, one process); it takes ~20s
// because a live geph5 session must come up.
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in + external outbound plugins (incl. geph)
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_GephProviderStandalone provisions the geph provider in embedded_exit
// mode and proxies an HTTP request through engine → client → embedded exit →
// loopback target, all in one process, asserting the response body and the
// recorded metric.
func TestE2E_GephProviderStandalone(t *testing.T) {
	// 1. The "internet": a deterministic HTTP server on loopback.
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	targetPort := targetLn.Addr().(*net.TCPAddr).Port
	target := fmt.Sprintf("127.0.0.1:%d", targetPort)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "standalone-goose-e2e:%s", r.URL.Path)
	})}
	go srv.Serve(targetLn)
	defer srv.Close()

	// 2. Engine + admin API.
	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	// 3. Provision the geph provider in standalone mode: embedded_exit boots
	//    the geph5 exit inside the engine's process (allow_loopback so it can
	//    reach the loopback target — a deployed exit would face the real
	//    internet) and pins the embedded client at it. No broker, no external
	//    exit, no exit_constraint needed.
	resp, body := api.do(http.MethodPost, "/api/providers", config.ProviderSpec{
		ID:       "geph1",
		Provider: "geph",
		Config:   map[string]any{"embedded_exit": map[string]any{"allow_loopback": true}, "secret": "goose-e2e-standalone"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create provider: %s", body)

	// 4. Wait for the provider's socks5 outbound to land in the default pool.
	//    This is the slow part: a real session to the embedded exit must come
	//    up (real handshake, real tunnel — only the network path is loopback).
	outboundID := "geph"
	require.Eventually(t, func() bool {
		resp, body = api.do(http.MethodGet, "/api/pools", nil)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var pools []config.Pool
		if err := json.Unmarshal(body, &pools); err != nil {
			return false
		}
		for _, p := range pools {
			if p.ID != config.DefaultPoolID {
				continue
			}
			for _, id := range p.OutboundIDs {
				if id == outboundID {
					return true
				}
			}
		}
		return false
	}, 150*time.Second, 500*time.Millisecond, "geph provider outbound never landed in the default pool")

	// 5. An authenticated HTTP inbound routed through the default pool, with
	//    a protocol filter that allows the provider's socks5 outbound.
	port := freePort(t)
	resp, body = api.do(http.MethodPost, "/api/inbounds", config.Inbound{
		ID: "geph-in", Protocol: "http",
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Users:  []config.User{{Username: "app", Password: "secret"}},
		Policy: config.InboundPolicy{
			PoolIDs: []string{config.DefaultPoolID},
			Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []string{"socks5"}}}},
		},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create inbound: %s", body)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", port)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond, "inbound listener did not come up")

	// 6. Push the request through the whole chain: engine → socks5 outbound →
	//    embedded client → tunnel → embedded exit → loopback target.
	cli := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse("http://app:secret@" + proxyAddr)
		},
	}}
	respHttp, err := cli.Get("http://" + target + "/via-geph")
	require.NoError(t, err, "request through goose → geph tunnel → embedded exit failed")
	defer respHttp.Body.Close()
	got, err := io.ReadAll(respHttp.Body)
	require.NoError(t, err, "read body")
	assert.Equal(t, http.StatusOK, respHttp.StatusCode)
	assert.Equal(t, "standalone-goose-e2e:/via-geph", string(got), "body through the one-process chain")
	t.Logf("one-process chain delivered: %q", string(got))

	// 7. The request must be recorded as a metric routed through the geph
	//    outbound, queryable via the API.
	require.Eventually(t, func() bool {
		resp, body = api.do(http.MethodGet, "/api/metrics?n=10", nil)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var recs []core.RequestMetric
		if err := json.Unmarshal(body, &recs); err != nil {
			return false
		}
		for _, rec := range recs {
			if rec.InboundID != "geph-in" || rec.User != "app" {
				continue
			}
			for _, hop := range rec.Chain {
				if strings.Contains(hop, outboundID) {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "metric for the geph-tunneled request was not recorded")
}
