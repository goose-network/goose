package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/inbound"
	_ "github.com/goose-network/goose/plugins/socks5" // real SOCKS5 outbound plugin
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fpDirect is an inbound handler with unrestricted direct egress — the relay
// proxy server in the full-probe integration test.
type fpDirect struct{}

func (fpDirect) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", meta.Target())
	return c, nil, err
}

func (fpDirect) Bridge(in, up net.Conn, _ core.Metadata) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, in); done <- struct{}{} }()
	go func() { _, _ = io.Copy(in, up); done <- struct{}{} }()
	<-done
	_ = in.Close()
	_ = up.Close()
}

// fpFlaky wraps a handler and refuses its first CONNECT, then serves every
// later one. It stands in for the router fixture's blocked stack (which
// engine.New cannot inject): the depth-1 local→work attempt is work's first
// CONNECT and fails, while the depth-2 relay→work rescue is its second and
// succeeds — deterministic regardless of attempt ordering, since only work's
// own CONNECT count matters.
type fpFlaky struct {
	inner inbound.Handler
	n     atomic.Int64
}

func (f *fpFlaky) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	if f.n.Add(1) == 1 {
		return nil, nil, fmt.Errorf("work: first CONNECT refused (simulated local block)")
	}
	return f.inner.Dial(ctx, meta)
}

func (f *fpFlaky) Bridge(in, up net.Conn, m core.Metadata) { f.inner.Bridge(in, up, m) }

// fpAPI is a small JSON client for the engine's admin API.
func fpAPI(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, url, rdr)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// TestEngineFullProbeApplyRoutesTraffic is the goal workflow end to end
// against a real engine: the local network blocks the work proxy, a full
// active probing job discovers local→relay and local→relay→work, apply turns
// those paths into chains + inbounds, the engine hot-starts the listeners,
// and real traffic through each generated inbound reaches the target.
func TestEngineFullProbeApplyRoutesTraffic(t *testing.T) {
	// Target: an HTTP server that answers with a marker body.
	var hits int64
	var hitsMu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsMu.Lock()
		hits++
		hitsMu.Unlock()
		fmt.Fprint(w, "full-probe-ok")
	}))
	t.Cleanup(target.Close)

	// Work: a real SOCKS5 server whose FIRST CONNECT is refused (fpFlaky),
	// so the depth-1 local→work attempt fails while the depth-2 relay→work
	// rescue — work's second CONNECT — succeeds.
	workLn, err := inbound.New("fp-work", "socks5", "127.0.0.1:0", inbound.NewAuthStore(nil), func(string) string { return "" }, &fpFlaky{inner: fpDirect{}})
	require.NoError(t, err)
	t.Cleanup(func() { workLn.Close() })

	// Relay: an unrestricted SOCKS5 server.
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
	// StartAPI serves asynchronously: wait for the bind before the first
	// request, or a slow listen under -race load answers connection refused.
	waitFor(t, 5*time.Second, func() bool {
		c, err := net.DialTimeout("tcp", apiAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	})

	// Step 1+2: set the target and start the full active probing job.
	code, body := fpAPI(t, "POST", base+"/api/probes/full", map[string]any{
		"target": target.URL, "timeout_seconds": 5, "rps": 1000, "max_depth": 5,
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	var j core.FullProbeJob
	require.NoError(t, json.Unmarshal(body, &j))

	// Steps 3-6: poll until the sweep finishes (max depth 5 or no extension).
	require.Eventually(t, func() bool {
		code, body := fpAPI(t, "GET", base+"/api/probes/full/"+j.ID, nil)
		require.Equal(t, http.StatusOK, code)
		require.NoError(t, json.Unmarshal(body, &j))
		return j.State != core.FullProbeRunning
	}, 20*time.Second, 50*time.Millisecond, "sweep never finished")
	require.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)

	// Local→work was refused (first CONNECT), local→relay worked, and the
	// rescue local→relay→work was proved at depth 2.
	best := j.BestPaths()
	require.Len(t, best, 2)
	assert.Equal(t, []string{"relay"}, best[0].Hops)
	assert.Equal(t, []string{"relay", "work"}, best[1].Hops)

	// Step 7: create inbounds from the successful paths.
	code, body = fpAPI(t, "POST", base+"/api/probes/full/"+j.ID+"/inbounds", nil)
	require.Equal(t, http.StatusCreated, code, string(body))
	var applied struct {
		JobID   string                      `json:"job_id"`
		Results []core.FullProbeApplyResult `json:"results"`
	}
	require.NoError(t, json.Unmarshal(body, &applied))
	require.Len(t, applied.Results, 2)

	// The engine hot-starts the generated listeners; then real traffic
	// through EACH generated inbound must reach the target.
	for _, res := range applied.Results {
		require.Equal(t, "socks5", res.Protocol)
		waitFor(t, 5*time.Second, func() bool {
			e.mu.Lock()
			defer e.mu.Unlock()
			ln, ok := e.listeners[res.InboundID]
			return ok && ln.Address() != ""
		})

		client := &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// SOCKS5 handshake with the generated inbound, no auth.
				var d net.Dialer
				c, err := d.DialContext(ctx, "tcp", res.Listen)
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
		}}
		resp, err := client.Get(target.URL)
		require.NoError(t, err, "traffic through generated inbound %s", res.InboundID)
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "full-probe-ok", string(got), "via %v", res.Path.Hops)
		client.CloseIdleConnections()
	}

	hitsMu.Lock()
	assert.GreaterOrEqual(t, hits, int64(2), "target must have served both generated inbounds")
	hitsMu.Unlock()
}
