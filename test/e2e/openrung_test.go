// Real end-to-end test for the "openrung" provider plugin inside the goose
// engine, against the PRODUCTION OpenRung network: the provider fetches the
// signed relay directory from the real broker fronts (through the upstream
// SOCKS5 proxy when OPENRUNG_UPSTREAM is set — the deployment shape where
// broker traffic needs a forwarder), contributes one mihomo-vless outbound per
// usable relay to the default pool, and a real HTTP request rides the chain
//
//	HTTP client → goose http inbound → pool policy → mihomo-vless outbound →
//	  VLESS + REALITY + Vision tunnel → production relay → real internet target
//
// Run with (skipped unless OPENRUNG_E2E is set):
//
//	OPENRUNG_E2E=1 [OPENRUNG_UPSTREAM=127.0.0.1:7890] \
//	GEPH_TEST_TARGET=223.5.5.5:80 \
//	go test -run TestE2E_OpenRungProviderRealNetwork ./test/e2e/
//
// Relay dials are direct even with an upstream set (see the openrung plugin):
// a REALITY ClientHello carries its camouflage SNI as the address, so a
// TLS-sniffing forwarder would re-target relay dials to the camouflage site.
// The default target 223.5.5.5:80 (AliDNS) answers GET / with a deterministic
// 404.
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in + external outbound plugins (incl. openrung)
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_OpenRungProviderRealNetwork provisions the openrung provider via
// the admin API, waits for its relay outbounds to land in the default pool,
// and proxies a real HTTP request through the engine → plugin → REALITY
// tunnel → production relay → internet chain, asserting the response and the
// recorded metric.
func TestE2E_OpenRungProviderRealNetwork(t *testing.T) {
	if os.Getenv("OPENRUNG_E2E") == "" {
		t.Skip("OPENRUNG_E2E not set; skipping real openrung provider e2e")
	}
	target := os.Getenv("GEPH_TEST_TARGET")
	if target == "" {
		target = "223.5.5.5:80"
	}
	upstream := os.Getenv("OPENRUNG_UPSTREAM")

	// 1. Engine + admin API.
	eng, api := newEngineWithAPI(t, "")
	defer eng.Close()

	// 2. Provision the openrung provider. Outbounds does a live directory
	//    fetch against the production broker; the upstream (when set) carries
	//    the broker traffic.
	providerCfg := map[string]any{
		// The full relay rotation: the broker's default page is a random
		// slice of it, which can exclude every relay reachable from this
		// host and starve the retry loop below.
		"limit": 20,
	}
	if upstream != "" {
		providerCfg["upstream_proxy"] = upstream
	}
	resp, body := api.do(http.MethodPost, "/api/providers", config.ProviderSpec{
		ID:       "openrung1",
		Provider: "openrung",
		Config:   providerCfg,
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, "create provider: %s", body)

	// 3. Wait for the provider's relay outbounds ("openrung-<relay id>") to
	//    land in the default pool.
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
				if strings.HasPrefix(id, "openrung-") {
					return true
				}
			}
		}
		return false
	}, 120*time.Second, time.Second, "openrung provider outbounds never landed in the default pool")

	// 4. An authenticated HTTP inbound routed through the default pool, with
	//    a protocol filter that allows the provider's mihomo-vless outbound.
	port := freePort(t)
	resp, body = api.do(http.MethodPost, "/api/inbounds", config.Inbound{
		ID: "openrung-in", Protocol: "http",
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Users:  []config.User{{Username: "app", Password: "secret"}},
		Policy: config.InboundPolicy{
			PoolIDs: []string{config.DefaultPoolID},
			Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []string{"mihomo-vless"}}}},
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

	// 5. Push a real request through the whole chain. The engine selects one
	//    relay outbound per request (rotation + stats — dialChain does not
	//    fail over inside a request); on a host where only some production
	//    relays are reachable, a request that lands on an unreachable relay
	//    fails with 502 and the next request rotates to the next candidate.
	//    A bounded retry loop is how real clients ride that rotation until a
	//    dialed relay answers.
	cli := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse("http://app:secret@" + proxyAddr)
		},
	}}
	var (
		respHttp *http.Response
		got      []byte
		err      error
	)
	// One attempt per relay in the rotation plus one repeat: dialChain does
	// not fail over within a request, but the pool's selector rotates per
	// request, so a full cycle is guaranteed to reach every candidate.
	for attempt := 1; attempt <= 10; attempt++ {
		respHttp, err = cli.Get("http://" + target + "/")
		if err != nil {
			t.Fatalf("attempt %d: request through goose → openrung REALITY tunnel → relay failed: %v", attempt, err)
		}
		got, _ = io.ReadAll(respHttp.Body)
		respHttp.Body.Close()
		if respHttp.StatusCode != http.StatusBadGateway {
			break // a relay dialed; the target answered
		}
		t.Logf("attempt %d: engine picked an unreachable relay (502); rotating", attempt)
	}
	require.NotNil(t, respHttp, "no request completed")
	t.Logf("target %s answered: status %d, body %q (%d bytes)", target, respHttp.StatusCode, string(got[:min(len(got), 80)]), len(got))
	assert.Equal(t, http.StatusNotFound, respHttp.StatusCode, "AliDNS GET / is a stable 404")
	assert.NotEmpty(t, got, "expected a response body from the real target")

	// 6. The request must be recorded as a metric routed through an openrung
	//    relay outbound, queryable via the API.
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
			if rec.InboundID != "openrung-in" || rec.User != "app" {
				continue
			}
			for _, hop := range rec.Chain {
				if strings.HasPrefix(hop, "openrung-") {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "metric for the openrung-tunneled request was not recorded")
}
