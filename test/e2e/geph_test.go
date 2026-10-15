// Real end-to-end test for the "geph" provider plugin inside the goose engine.
// It boots a REAL geph5 exit (the Rust geph5-exit binary), provisions the
// provider through the admin API, waits for its SOCKS5 outbound to land in the
// managed pool (which requires a real live geph5 session — X25519+ed25519
// handshake, picomux tunnel — to the exit), then pushes a real HTTP request
// through the whole chain:
//
//	HTTP client → goose http inbound → pool policy → socks5 outbound →
//	  embedded geph5 client (plugin) → encrypted tunnel → Rust geph5-exit →
//	  real internet target
//
// Run with (skipped unless GEPH_EXIT_BIN is set):
//
//	GEPH_EXIT_BIN=/path/to/geph5-exit \
//	GEPH_TEST_TARGET=223.5.5.5:80 \
//	go test -run TestE2E_Geph ./test/e2e/
//
// GEPH_TEST_TARGET must be an IP-literal host:port the exit can reach and is
// allowed to proxy to (globally routable — the exit refuses private/loopback
// targets by design). The default 223.5.5.5:80 (AliDNS) answers GET / with a
// deterministic 404.
package e2e

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in + external outbound plugins (incl. geph)
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_GephProviderRealTunnel provisions the geph provider via the admin
// API against a real geph5-exit process and proxies a real HTTP request
// through the engine → plugin → tunnel → exit → internet chain, asserting the
// response body and the recorded metric.
func TestE2E_GephProviderRealTunnel(t *testing.T) {
	bin := os.Getenv("GEPH_EXIT_BIN")
	if bin == "" {
		t.Skip("GEPH_EXIT_BIN not set; skipping real geph provider e2e")
	}
	target := os.Getenv("GEPH_TEST_TARGET")
	if target == "" {
		target = "223.5.5.5:80"
	}

	// 1. Boot a real geph5-exit: fresh signing seed (so the test derives its
	//    pubkey), free loopback ports, no broker (handle_client then skips
	//    credential verification).
	dir := t.TempDir()
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	secretPath := filepath.Join(dir, "signing_secret")
	if err := os.WriteFile(secretPath, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	c2e, b2e := freePort(t), freePort(t)
	cfgPath := filepath.Join(dir, "exit.yaml")
	cfg := fmt.Sprintf(`
signing_secret: %s
c2e_listen: 127.0.0.1:%d
b2e_listen: 127.0.0.1:%d
country: US
city: test
`, secretPath, c2e, b2e)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	exitCmd := exec.Command(bin, "--config", cfgPath)
	exitLog, _ := os.Create(filepath.Join(dir, "exit.log"))
	exitCmd.Stdout, exitCmd.Stderr = exitLog, exitLog
	if err := exitCmd.Start(); err != nil {
		t.Fatalf("start geph5-exit: %v", err)
	}
	defer func() {
		_ = exitCmd.Process.Kill()
		_, _ = exitCmd.Process.Wait()
		exitLog.Close()
	}()
	c2eAddr := fmt.Sprintf("127.0.0.1:%d", c2e)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", c2eAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 30*time.Second, 100*time.Millisecond, "exit c2e listener did not come up")

	// The exit's ed25519 pubkey, derived from the seed we pre-created (the
	// exit uses the 32-byte file contents as its signing key seed).
	pubHex := hex.EncodeToString(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey))

	// 2. Engine + admin API.
	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	// 3. Provision the geph provider with a direct exit constraint (no broker
	//    needed). Its embedded geph5 client dials the exit, completes the
	//    real handshake, and — once a session is live — contributes one
	//    socks5 outbound to the default pool.
	resp, body := api.do(http.MethodPost, "/api/providers", config.ProviderSpec{
		ID:       "geph1",
		Provider: "geph",
		Config:   map[string]any{"exit_constraint": map[string]any{"direct": fmt.Sprintf("%s/%s", c2eAddr, pubHex)}, "secret": "goose-e2e"},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create provider: %s", body)

	// 4. Wait for the provider's socks5 outbound to land in the default pool.
	//    This is the slow part: a real session must come up.
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
			PoolID:  config.DefaultPoolID,
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

	// 6. Push a real request through the whole chain. The engine dials the
	//    provider's SOCKS5 outbound, whose embedded geph5 client tunnels it
	//    through the exit to the real internet target.
	cli := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse("http://app:secret@" + proxyAddr)
		},
	}}
	respHttp, err := cli.Get("http://" + target + "/")
	require.NoError(t, err, "request through goose → geph tunnel → exit failed")
	defer respHttp.Body.Close()
	got, _ := io.ReadAll(respHttp.Body)
	t.Logf("target %s answered: status %d, body %q (%d bytes)", target, respHttp.StatusCode, string(got[:min(len(got), 80)]), len(got))
	assert.Equal(t, http.StatusNotFound, respHttp.StatusCode, "AliDNS GET / is a stable 404")
	assert.NotEmpty(t, got, "expected a response body from the real target")

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
