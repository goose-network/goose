package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/inbound"
	_ "github.com/goose-network/goose/plugins/socks5" // real SOCKS5 outbound plugin
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fpSocksTransport returns an HTTP transport that reaches addr through the
// SOCKS5 listener at listen (no auth), one fresh connection per request so
// every request exercises the pool's selection anew.
func fpSocksTransport(listen string) *http.Transport {
	return &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			c, err := d.DialContext(ctx, "tcp", listen)
			if err != nil {
				return nil, err
			}
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := c.Write([]byte{0x05, 0x01, 0x00}); err != nil {
				c.Close()
				return nil, err
			}
			rep := make([]byte, 2)
			if _, err := io.ReadFull(c, rep); err != nil || rep[0] != 0x05 || rep[1] != 0x00 {
				c.Close()
				return nil, fmt.Errorf("socks5 greeting: %v %v", rep, err)
			}
			host, portStr, err := net.SplitHostPort(addr)
			if err != nil {
				c.Close()
				return nil, err
			}
			var port int
			fmt.Sscanf(portStr, "%d", &port)
			req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
			req = append(req, host...)
			req = append(req, byte(port>>8), byte(port))
			if _, err := c.Write(req); err != nil {
				c.Close()
				return nil, err
			}
			head := make([]byte, 4)
			if _, err := io.ReadFull(c, head); err != nil || head[1] != 0x00 {
				c.Close()
				return nil, fmt.Errorf("socks5 connect: %v %v", head, err)
			}
			var skip int
			switch head[3] {
			case 0x01:
				skip = 4 + 2
			case 0x04:
				skip = 16 + 2
			case 0x03:
				l := make([]byte, 1)
				if _, err := io.ReadFull(c, l); err != nil {
					c.Close()
					return nil, err
				}
				skip = int(l[0]) + 2
			}
			if _, err := io.ReadFull(c, make([]byte, skip)); err != nil {
				c.Close()
				return nil, err
			}
			_ = c.SetDeadline(time.Time{})
			return c, nil
		},
	}
}

// TestEngineFullProbePoolApplyRoutesTraffic is the pool-mode goal workflow
// end to end against a real engine: the same blocked-work fixture as the
// inbounds test, but apply builds ONE pool over the proved paths behind a
// SINGLE inbound, and repeated real traffic through that one listener rides
// BOTH member chains (roundrobin), reaching the target every time.
func TestEngineFullProbePoolApplyRoutesTraffic(t *testing.T) {
	var hits int64
	var hitsMu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsMu.Lock()
		hits++
		hitsMu.Unlock()
		fmt.Fprint(w, "full-probe-ok")
	}))
	t.Cleanup(target.Close)

	workLn, err := inbound.New("fp-work", "socks5", "127.0.0.1:0", inbound.NewAuthStore(nil), func(string) string { return "" }, &fpFlaky{inner: fpDirect{}})
	require.NoError(t, err)
	t.Cleanup(func() { workLn.Close() })
	relayLn, err := inbound.New("fp-relay", "socks5", "127.0.0.1:0", inbound.NewAuthStore(nil), func(string) string { return "" }, fpDirect{})
	require.NoError(t, err)
	t.Cleanup(func() { relayLn.Close() })

	cfg := config.NewStore()
	apiAddr := freeAddr(t)
	cfg.SetEngine(config.Engine{
		Stack: "system",
		DB:    t.TempDir() + "/engine.db",
		API:   config.APIConfig{Listen: apiAddr},
	})
	cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5", Config: map[string]any{"address": relayLn.Address()}})
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5", Config: map[string]any{"address": workLn.Address()}})

	e, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	require.NoError(t, e.StartAPI())
	base := "http://" + apiAddr
	waitFor(t, 5*time.Second, func() bool {
		c, err := net.DialTimeout("tcp", apiAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	})

	code, body := fpAPI(t, "POST", base+"/api/probes/full", map[string]any{
		"target": target.URL, "timeout_seconds": 5, "rps": 1000, "max_depth": 5,
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	var j core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &j))
	require.Eventually(t, func() bool {
		code, body := fpAPI(t, "GET", base+"/api/probes/full/"+j.ID, nil)
		require.Equal(t, http.StatusOK, code)
		require.NoError(t, json.Unmarshal(body, &j))
		return j.State != core.FullProbeRunning
	}, 20*time.Second, 50*time.Millisecond, "sweep never finished")
	require.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)

	// Pool-mode apply: one pool over both proved paths, one inbound.
	code, body = fpAPI(t, "POST", base+"/api/probes/full/"+j.ID+"/inbounds", map[string]any{"mode": "pool"})
	require.Equal(t, http.StatusCreated, code, string(body))
	var applied struct {
		JobID     string                      `json:"job_id"`
		Results   []core.FullProbeApplyResult `json:"results"`
		PoolID    string                      `json:"pool_id"`
		InboundID string                      `json:"inbound_id"`
		Listen    string                      `json:"listen"`
		Protocol  string                      `json:"protocol"`
	}
	require.NoError(t, json.Unmarshal(body, &applied))
	require.Len(t, applied.Results, 2)
	assert.NotEmpty(t, applied.PoolID)
	assert.NotEmpty(t, applied.InboundID)
	assert.Equal(t, "socks5", applied.Protocol)
	for _, res := range applied.Results {
		assert.Empty(t, res.InboundID, "pool mode has no per-chain inbound")
	}

	// The engine hot-starts the ONE generated listener.
	waitFor(t, 5*time.Second, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		ln, ok := e.listeners[applied.InboundID]
		return ok && ln.Address() != ""
	})
	e.mu.Lock()
	require.Len(t, e.listeners, 1, "pool mode must start exactly one listener")
	e.mu.Unlock()

	// Repeated traffic through the single inbound: roundrobin over the two
	// member chains means four requests ride each path twice.
	client := &http.Client{Transport: fpSocksTransport(applied.Listen)}
	for i := 0; i < 4; i++ {
		resp, err := client.Get(target.URL)
		require.NoError(t, err, "request %d through pool inbound", i)
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "full-probe-ok", string(got))
	}
	client.CloseIdleConnections()

	hitsMu.Lock()
	assert.GreaterOrEqual(t, hits, int64(4), "target must have served every pooled request")
	hitsMu.Unlock()

	// The request rows prove BOTH member chains carried traffic: the chain
	// recorded is the real hop list, [relay] and [relay work].
	require.Eventually(t, func() bool {
		code, body := fpAPI(t, "GET", base+"/api/metrics?n=200", nil)
		require.Equal(t, http.StatusOK, code)
		var rows []core.RequestMetric
		require.NoError(t, json.Unmarshal(body, &rows))
		seen := map[int]int{}
		for _, m := range rows {
			if m.InboundID == applied.InboundID && m.Success {
				seen[len(m.Chain)]++
			}
		}
		return seen[1] >= 2 && seen[2] >= 2
	}, 5*time.Second, 100*time.Millisecond, "both member chains must carry traffic")
}
