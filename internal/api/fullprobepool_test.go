package api

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullProbeApplyPoolDefaults asserts the pool-mode apply with only
// mode=pool builds ALL proved paths fastest-first as member chains, one pool
// over them, and ONE inbound routed through the pool — and the response
// describes the listener at the top level, with empty inbound fields on the
// per-chain results.
func TestFullProbeApplyPoolDefaults(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{"mode": "pool"})
	require.Equal(t, http.StatusCreated, code, string(body))
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "fp-test", resp.JobID)
	assert.Equal(t, "fullprobe-fp-test-pool", resp.PoolID)
	assert.Equal(t, "fullprobe-fp-test", resp.InboundID)
	assert.Equal(t, "socks5", resp.Protocol)
	require.NotNil(t, resp.Selector)
	assert.Equal(t, "roundrobin", resp.Selector.Type)
	assert.True(t, strings.HasPrefix(resp.Listen, "127.0.0.1:"), "listen %q", resp.Listen)
	portStr := strings.TrimPrefix(resp.Listen, "127.0.0.1:")
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	assert.Greater(t, port, 0, "a concrete free port must be named")

	// Fastest first: relay (10ms) before relay→work (20ms).
	require.Len(t, resp.Results, 2)
	assert.Equal(t, []string{"relay"}, resp.Results[0].Path.Hops)
	assert.Equal(t, "fullprobe-fp-test-d1-1", resp.Results[0].ChainID)
	assert.Equal(t, []string{"relay", "work"}, resp.Results[1].Path.Hops)
	assert.Equal(t, "fullprobe-fp-test-d2-2", resp.Results[1].ChainID)
	for _, res := range resp.Results {
		assert.Empty(t, res.InboundID, "pool mode has no per-chain inbound")
		assert.Empty(t, res.Listen)
		assert.Empty(t, res.Protocol)
		ch, ok := cfg.Chain(res.ChainID)
		require.True(t, ok, "chain %s missing from config", res.ChainID)
		assert.Equal(t, res.Path.Hops, ch.Layers)
	}

	p, ok := cfg.Pool(resp.PoolID)
	require.True(t, ok, "pool missing from config")
	assert.Equal(t, []string{"fullprobe-fp-test-d1-1", "fullprobe-fp-test-d2-2"}, p.ChainIDs)
	assert.Empty(t, p.OutboundIDs)
	assert.Equal(t, "roundrobin", p.Selector.Type)

	require.Len(t, cfg.Inbounds(), 1, "pool mode creates exactly one inbound")
	in, ok := cfg.Inbound(resp.InboundID)
	require.True(t, ok)
	assert.Equal(t, "socks5", in.Protocol)
	assert.Equal(t, resp.Listen, in.Listen)
	assert.Equal(t, resp.PoolID, in.Policy.PoolID)
	assert.Empty(t, in.Policy.ChainID)
}

// TestFullProbeApplyPoolExplicit asserts an explicit pool-mode selection
// applies exactly the named paths under the requested pool_id, selector,
// protocol, port, and id_prefix.
func TestFullProbeApplyPoolExplicit(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"mode": "pool", "paths": [][]string{{"relay", "work"}},
		"pool_id": "rescued", "id_prefix": "rescue", "protocol": "http",
		"port": 23120, "selector": map[string]any{"type": "sticky"},
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Results, 1)
	assert.Equal(t, "rescue-d2-1", resp.Results[0].ChainID)
	assert.Equal(t, "rescued", resp.PoolID)
	assert.Equal(t, "rescue", resp.InboundID)
	assert.Equal(t, "127.0.0.1:23120", resp.Listen)
	assert.Equal(t, "http", resp.Protocol)
	require.NotNil(t, resp.Selector)
	assert.Equal(t, "sticky", resp.Selector.Type)

	ch, ok := cfg.Chain("rescue-d2-1")
	require.True(t, ok)
	assert.Equal(t, []string{"relay", "work"}, ch.Layers)
	p, ok := cfg.Pool("rescued")
	require.True(t, ok)
	assert.Equal(t, []string{"rescue-d2-1"}, p.ChainIDs)
	assert.Equal(t, "sticky", p.Selector.Type)
	in, ok := cfg.Inbound("rescue")
	require.True(t, ok)
	assert.Equal(t, "rescued", in.Policy.PoolID)
	require.Len(t, cfg.Inbounds(), 1)
}

// TestFullProbeApplyPoolRejections covers the pool-mode guards. Every case
// runs against a fresh fixture and asserts the rejection wrote NOTHING —
// validation must complete before the first config write.
func TestFullProbeApplyPoolRejections(t *testing.T) {
	// setup optionally seeds extra config before the apply; want counts are
	// the chains/pools/inbounds that must exist afterwards (the seeds only).
	cases := []struct {
		name       string
		body       map[string]any
		setup      func(t *testing.T, cfg *config.Store)
		wantCode   int
		wantChains int
		wantPools  int
		wantIns    int
	}{
		{name: "bad mode", body: map[string]any{"mode": "mesh"}, wantCode: http.StatusBadRequest},
		{name: "bad selector", body: map[string]any{"mode": "pool", "selector": map[string]any{"type": "bogus"}}, wantCode: http.StatusBadRequest},
		{name: "unproved path", body: map[string]any{"mode": "pool", "paths": [][]string{{"work"}}}, wantCode: http.StatusBadRequest},
		{name: "bad protocol", body: map[string]any{"mode": "pool", "protocol": "vmess"}, wantCode: http.StatusBadRequest},
		{name: "port out of range", body: map[string]any{"mode": "pool", "port": 70000}, wantCode: http.StatusBadRequest},
		{
			name: "too many explicit members",
			body: func() map[string]any {
				paths := make([][]string, core.FullProbeMaxApplyPoolMembers+1)
				for i := range paths {
					paths[i] = []string{"relay"}
				}
				return map[string]any{"mode": "pool", "paths": paths}
			}(),
			wantCode: http.StatusBadRequest,
		},
		{
			name: "pool exists", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.SetPool(&config.Pool{ID: "fullprobe-fp-test-pool", OutboundIDs: []string{"relay"}})
			},
			wantCode: http.StatusConflict, wantPools: 1,
		},
		{
			name: "pool_id shadows outbound", body: map[string]any{"mode": "pool", "pool_id": "relay"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "pool_id collides with generated chain", body: map[string]any{"mode": "pool", "pool_id": "fullprobe-fp-test-d1-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "inbound prefix exists", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.SetInbound(&config.Inbound{ID: "fullprobe-fp-test", Protocol: "socks5", Listen: "127.0.0.1:1"})
			},
			wantCode: http.StatusConflict, wantIns: 1,
		},
		{
			name: "chain id exists", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.SetChain(&config.ChainSpec{ID: "fullprobe-fp-test-d1-1", Layers: []string{"relay"}})
			},
			wantCode: http.StatusConflict, wantChains: 1,
		},
		{
			name: "inbound named like generated chain", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.SetInbound(&config.Inbound{ID: "fullprobe-fp-test-d1-1", Protocol: "socks5", Listen: "127.0.0.1:1"})
			},
			wantCode: http.StatusConflict, wantIns: 1,
		},
		{
			name: "hop deleted", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.DeleteOutbound("work")
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "hop shadowed by pool", body: map[string]any{"mode": "pool"},
			setup: func(t *testing.T, cfg *config.Store) {
				cfg.SetPool(&config.Pool{ID: "work", OutboundIDs: []string{"relay"}})
			},
			wantCode: http.StatusBadRequest, wantPools: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fr := &fakeFullRunner{}
			fr.set(finishedJob())
			hs, cfg, _ := newFullProbeServer(t, fr)
			if tc.setup != nil {
				tc.setup(t, cfg)
			}
			body := tc.body
			code, respBody := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", body)
			assert.Equal(t, tc.wantCode, code, string(respBody))
			assert.Len(t, cfg.Chains(), tc.wantChains, "a rejected apply must not write chains")
			assert.Len(t, cfg.Pools(), tc.wantPools, "a rejected apply must not write pools")
			assert.Len(t, cfg.Inbounds(), tc.wantIns, "a rejected apply must not write inbounds")
		})
	}
}

// TestFullProbeApplyPoolPortCollision asserts an explicit port already bound
// by a live listener is refused (409) in pool mode too, writing nothing.
func TestFullProbeApplyPoolPortCollision(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	port := ln.Addr().(*net.TCPAddr).Port
	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{
		"mode": "pool", "port": port,
	})
	assert.Equal(t, http.StatusConflict, code, string(body))
	assert.Empty(t, cfg.Chains())
	assert.Empty(t, cfg.Pools())
	assert.Empty(t, cfg.Inbounds())
}

// TestFullProbeApplyPoolNoPaths asserts a finished job with no successful
// paths answers 400 in pool mode too — there is nothing to load-balance.
func TestFullProbeApplyPoolNoPaths(t *testing.T) {
	fr := &fakeFullRunner{}
	empty := finishedJob()
	empty.Levels = nil
	fr.set(empty)
	hs, cfg, _ := newFullProbeServer(t, fr)
	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{"mode": "pool"})
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	assert.Empty(t, cfg.Chains())
	assert.Empty(t, cfg.Pools())
	assert.Empty(t, cfg.Inbounds())
}

// TestFullProbeApplyPoolMemberCap asserts the DEFAULT selection truncates to
// the member cap fastest-first: 300 proved one-hop paths yield exactly 256
// member chains, the 256 fastest.
func TestFullProbeApplyPoolMemberCap(t *testing.T) {
	fr := &fakeFullRunner{}
	hs, cfg, _ := newFullProbeServer(t, fr)
	j := finishedJob()
	j.Levels = []core.FullProbeLevel{{Depth: 1}}
	const n = core.FullProbeMaxApplyPoolMembers + 44
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("o%03d", i)
		cfg.SetOutbound(&config.OutboundSpec{ID: id, Protocol: "socks5"})
		j.Levels[0].SuccessPaths = append(j.Levels[0].SuccessPaths, core.FullProbePath{
			Hops: []string{id}, Latency: time.Duration(i+1) * time.Millisecond, Status: 200,
		})
	}
	fr.set(j)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", map[string]any{"mode": "pool"})
	require.Equal(t, http.StatusCreated, code, string(body))
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Results, core.FullProbeMaxApplyPoolMembers)
	for i, res := range resp.Results {
		assert.Equal(t, []string{fmt.Sprintf("o%03d", i)}, res.Path.Hops, "member %d must be the %d-th fastest path", i, i)
	}
	p, ok := cfg.Pool(resp.PoolID)
	require.True(t, ok)
	assert.Len(t, p.ChainIDs, core.FullProbeMaxApplyPoolMembers)
	assert.Len(t, cfg.Chains(), core.FullProbeMaxApplyPoolMembers)
	assert.Len(t, cfg.Inbounds(), 1)
}

// TestFullProbeApplyInboundsResponseUnchanged pins the additive wire
// contract: the default (inbounds) apply response carries NONE of the
// pool-mode top-level keys, so existing clients see byte-identical JSON.
func TestFullProbeApplyInboundsResponseUnchanged(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, _, _ := newFullProbeServer(t, fr)

	code, body := do(t, hs, "POST", "/api/probes/full/fp-test/inbounds", "", nil)
	require.Equal(t, http.StatusCreated, code, string(body))
	var top map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &top))
	for _, key := range []string{"pool_id", "inbound_id", "listen", "protocol", "selector"} {
		_, present := top[key]
		assert.False(t, present, "inbounds-mode response must not carry top-level %q", key)
	}
	var resp fullProbeApplyResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Results, 2)
	for _, res := range resp.Results {
		assert.NotEmpty(t, res.InboundID, "inbounds mode keeps per-path inbound fields")
		assert.NotEmpty(t, res.Listen)
		assert.NotEmpty(t, res.Protocol)
	}
}

// TestPoolsChainMembersValidation covers the pools write-time guard: chain
// members must name existing non-empty chains of pinned, unshadowed
// outbounds, with IDs distinct across both member lists — while plain pools
// keep their pre-existing acceptance untouched.
func TestPoolsChainMembersValidation(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetOutbound(&config.OutboundSpec{ID: "a", Protocol: "socks5"})
	cfg.SetOutbound(&config.OutboundSpec{ID: "b", Protocol: "socks5"})
	cfg.SetPool(&config.Pool{ID: "plainpool", OutboundIDs: []string{"a"}})
	cfg.SetChain(&config.ChainSpec{ID: "c1", Layers: []string{"a", "b"}})
	cfg.SetChain(&config.ChainSpec{ID: "cempty"})
	cfg.SetChain(&config.ChainSpec{ID: "cpool", Layers: []string{"plainpool"}})
	cfg.SetChain(&config.ChainSpec{ID: "cghost", Layers: []string{"a", "ghost"}})

	// Valid mixed pool: chain members plus plain members, IDs distinct.
	code, body := do(t, hs, "POST", "/api/pools", "", map[string]any{
		"id": "pp", "outbound_ids": []string{"a"}, "chain_ids": []string{"c1"},
		"selector": map[string]any{"type": "roundrobin"},
	})
	require.Equal(t, http.StatusCreated, code, string(body))
	p, ok := cfg.Pool("pp")
	require.True(t, ok)
	assert.Equal(t, []string{"c1"}, p.ChainIDs)
	code, body = do(t, hs, "GET", "/api/pools/pp", "", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), `"chain_ids":["c1"]`)

	rejections := []struct {
		name string
		pool map[string]any
	}{
		{"missing chain", map[string]any{"id": "bad", "chain_ids": []string{"nope"}}},
		{"empty chain", map[string]any{"id": "bad", "chain_ids": []string{"cempty"}}},
		{"pool layer", map[string]any{"id": "bad", "chain_ids": []string{"cpool"}}},
		{"unbuilt layer", map[string]any{"id": "bad", "chain_ids": []string{"cghost"}}},
		{"duplicate chain id", map[string]any{"id": "bad", "chain_ids": []string{"c1", "c1"}}},
		{"id in both lists", map[string]any{"id": "bad", "outbound_ids": []string{"c1"}, "chain_ids": []string{"c1"}}},
		{"duplicate outbound id", map[string]any{"id": "bad", "outbound_ids": []string{"a", "a"}, "chain_ids": []string{"c1"}}},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			code, body := do(t, hs, "POST", "/api/pools", "", tc.pool)
			assert.Equal(t, http.StatusBadRequest, code, string(body))
			_, exists := cfg.Pool("bad")
			assert.False(t, exists, "a rejected pool write must not store")
		})
	}

	// Plain pools keep their pre-existing acceptance: no member checks.
	code, body = do(t, hs, "POST", "/api/pools", "", map[string]any{
		"id": "plain2", "outbound_ids": []string{"ghost", "ghost"},
	})
	assert.Equal(t, http.StatusCreated, code, string(body))
}

// TestChainsPathPoolLayerRejected pins the write-time direction of rule 2: a
// chain may not traverse a path-pool (a pool with chain members), while a
// plain-pool layer keeps its pre-existing acceptance.
func TestChainsPathPoolLayerRejected(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetOutbound(&config.OutboundSpec{ID: "a", Protocol: "socks5"})
	cfg.SetChain(&config.ChainSpec{ID: "c1", Layers: []string{"a"}})
	cfg.SetPool(&config.Pool{ID: "pathpool", ChainIDs: []string{"c1"}})
	cfg.SetPool(&config.Pool{ID: "plainpool", OutboundIDs: []string{"a"}})

	code, body := do(t, hs, "POST", "/api/chains", "", map[string]any{
		"id": "bad", "layers": []string{"a", "pathpool"},
	})
	assert.Equal(t, http.StatusBadRequest, code, string(body))
	_, exists := cfg.Chain("bad")
	assert.False(t, exists, "a rejected chain write must not store")

	code, body = do(t, hs, "POST", "/api/chains", "", map[string]any{
		"id": "ok", "layers": []string{"plainpool"},
	})
	assert.Equal(t, http.StatusCreated, code, string(body))
}
