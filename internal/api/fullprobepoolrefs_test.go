package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoolsSelfShadowLayerRejected pins the self-shadow guard: a member-chain
// layer naming the pool being written would resolve to the pool itself once
// the write lands (routing resolves pools first), nesting selection and
// making rebuild drop the member. The write must be refused even when an
// outbound of that name exists — the exact shape that used to be accepted
// and silently shadow the hop.
func TestPoolsSelfShadowLayerRejected(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	// The outbound exists: without the self check the layer passes the
	// exists-as-outbound test and the pool write creates its own shadow.
	cfg.SetOutbound(&config.OutboundSpec{ID: "selfp", Protocol: "socks5"})
	cfg.SetChain(&config.ChainSpec{ID: "cs", Layers: []string{"selfp"}})

	for _, method := range []string{"POST", "PUT"} {
		t.Run(method, func(t *testing.T) {
			path := "/api/pools"
			if method == "PUT" {
				path = "/api/pools/selfp"
			}
			code, body := do(t, hs, method, path, "", map[string]any{
				"id": "selfp", "chain_ids": []string{"cs"},
			})
			assert.Equal(t, http.StatusBadRequest, code, string(body))
			assert.Contains(t, string(body), "is the pool itself")
			_, exists := cfg.Pool("selfp")
			assert.False(t, exists, "a rejected pool write must not store")
		})
	}

	// Same refusal when no outbound carries the name: the self check fires
	// before the generic not-an-outbound check, naming the real problem.
	cfg.DeleteOutbound("selfp")
	code, body := do(t, hs, "POST", "/api/pools", "", map[string]any{
		"id": "selfp", "chain_ids": []string{"cs"},
	})
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Contains(t, string(body), "is the pool itself")
}

// TestPoolsPathPoolConversionReverseScan pins the reverse direction of the
// path-pool layer rule: converting an existing PLAIN pool that a chain
// already references as a layer into a path-pool must be refused (the
// chains-write guard only fires on chain writes, so this ordering would
// otherwise slip through and rebuild would skip the referencing chain
// whole). Plain-pool writes keep working, and once the reference is gone the
// conversion succeeds.
func TestPoolsPathPoolConversionReverseScan(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetOutbound(&config.OutboundSpec{ID: "a", Protocol: "socks5"})
	cfg.SetChain(&config.ChainSpec{ID: "c1", Layers: []string{"a"}})

	// Plain pool P, then a chain traversing it — both legal today.
	code, body := do(t, hs, "POST", "/api/pools", "", map[string]any{
		"id": "P", "outbound_ids": []string{"a"},
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	code, body = do(t, hs, "POST", "/api/chains", "", map[string]any{
		"id": "user", "layers": []string{"P"},
	})
	require.Equal(t, http.StatusCreated, code, string(body))

	// Converting P to a path-pool while "user" traverses it: refused, and
	// the message must name the referencing chain so the operator can act.
	code, body = do(t, hs, "PUT", "/api/pools/P", "", map[string]any{
		"chain_ids": []string{"c1"},
	})
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	// Decode before matching: the raw JSON body escapes the quoted ids.
	var er errResponse
	require.NoError(t, json.Unmarshal(body, &er))
	assert.Contains(t, er.Error, `chain "user"`)
	p, ok := cfg.Pool("P")
	require.True(t, ok)
	assert.Empty(t, p.ChainIDs, "the rejected conversion must leave P plain")
	assert.Equal(t, []string{"a"}, p.OutboundIDs)

	// Plain-pool replacement writes keep today's acceptance: P stays a
	// legal chain layer while it has no chain members.
	code, body = do(t, hs, "PUT", "/api/pools/P", "", map[string]any{
		"outbound_ids": []string{"a"},
	})
	assert.Equal(t, http.StatusCreated, code, string(body))

	// Remove the reference, and the same conversion now succeeds.
	code, body = do(t, hs, "DELETE", "/api/chains/user", "", nil)
	require.Equal(t, http.StatusNoContent, code, string(body))
	code, body = do(t, hs, "PUT", "/api/pools/P", "", map[string]any{
		"chain_ids": []string{"c1"},
	})
	assert.Equal(t, http.StatusCreated, code, string(body))
	p, ok = cfg.Pool("P")
	require.True(t, ok)
	assert.Equal(t, []string{"c1"}, p.ChainIDs)
}

// TestFullProbeApplyPoolChainLayerRefRejected pins the apply-side reverse
// scan: a pool_id (explicit or derived from id_prefix) that an EXISTING chain
// references as a layer must be refused before any config write — creating
// the path-pool would make rebuild skip that chain whole and silently
// reroute its live traffic to the fallback.
func TestFullProbeApplyPoolChainLayerRefRejected(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
	}{
		{name: "explicit pool_id", body: map[string]any{"mode": "pool", "pool_id": "x-pool"}},
		{name: "derived from id_prefix", body: map[string]any{"mode": "pool", "id_prefix": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fr := &fakeFullRunner{}
			fr.set(finishedJob())
			hs, cfg, _ := newFullProbeServer(t, fr)
			// The reference dangles today (no pool or outbound named
			// x-pool); it is still a chain layer the new path-pool would
			// capture.
			cfg.SetChain(&config.ChainSpec{ID: "user", Layers: []string{"x-pool"}})

			code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", tc.body)
			assert.Equal(t, http.StatusBadRequest, code, string(body))
			var er errResponse
			require.NoError(t, json.Unmarshal(body, &er))
			assert.Contains(t, er.Error, `chain "user"`)
			assert.Len(t, cfg.Chains(), 1, "a rejected apply must not write chains")
			assert.Empty(t, cfg.Pools(), "a rejected apply must not write pools")
			assert.Empty(t, cfg.Inbounds(), "a rejected apply must not write inbounds")
			ch, ok := cfg.Chain("user")
			require.True(t, ok)
			assert.Equal(t, []string{"x-pool"}, ch.Layers, "the existing chain must be untouched")
		})
	}
}

// TestFullProbeApplyPoolHopRefNotFalsePositive asserts the reverse scan does
// not misfire on chains referencing HOPS: the generated member chains (and
// pre-existing chains over the same outbounds) never name poolID, so a legal
// pool-mode apply still succeeds with such chains present.
func TestFullProbeApplyPoolHopRefNotFalsePositive(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)
	cfg.SetChain(&config.ChainSpec{ID: "user", Layers: []string{"relay"}})

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{"mode": "pool"})
	require.Equal(t, http.StatusCreated, code, string(body))
	assert.Len(t, cfg.Chains(), 3, "the pre-existing chain plus two generated members")
	assert.Len(t, cfg.Pools(), 1)
	assert.Len(t, cfg.Inbounds(), 1)
}
