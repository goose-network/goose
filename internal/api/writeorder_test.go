package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stressDo issues one request from a stress goroutine. Unlike do, it never
// calls t.Fatalf — that must only run on the test goroutine — so a transport
// or marshal error is recorded with t.Errorf (goroutine-safe) and surfaces as
// status 0, which no invariant or success counter treats as a commit.
func stressDo(t *testing.T, hs *httptest.Server, method, path string, body any) int {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Errorf("marshal %s %s: %v", method, path, err)
		return 0
	}
	r, err := http.NewRequest(method, hs.URL+path, bytes.NewReader(b))
	if err != nil {
		t.Errorf("new request %s %s: %v", method, path, err)
		return 0
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := hs.Client().Do(r)
	if err != nil {
		t.Errorf("%s %s: %v", method, path, err)
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// pathPoolLayerViolation reports the forbidden end-state the write-time
// guards exist to prevent: pool X carries chain members (a path-pool) while
// chain U traverses X as a layer. Rebuild skips such a chain wholesale, so
// both guards must never let a concurrent pair land it.
func pathPoolLayerViolation(cfg *config.Store, poolID, chainID string) bool {
	p, ok := cfg.Pool(poolID)
	if !ok || len(p.ChainIDs) == 0 {
		return false
	}
	u, ok := cfg.Chain(chainID)
	if !ok {
		return false
	}
	for _, ref := range u.Layers {
		if ref == poolID {
			return true
		}
	}
	return false
}

// TestWriteOrderPoolConversionVsChainCreate pins the writeMu guarantee for
// the conversion direction: the pools write branch must hold the lock across
// validatePoolMembers' reverse scan AND SetPool, and the chains branch across
// its layer guard AND SetChain. Each round seeds a plain pool X and fires 40
// staggered writers — half converting X to a path-pool, half creating chain U
// with layers [X]. Either order alone is legal; the invariant is that the two
// commits never BOTH land. Pre-fix the check-then-write windows overlap and
// the forbidden state appears within a few hundred staggered pairs; with the
// lock the second direction's guard always sees the first commit and refuses.
func TestWriteOrderPoolConversionVsChainCreate(t *testing.T) {
	hs, cfg, _ := newAPIServer(t, "")
	cfg.SetOutbound(&config.OutboundSpec{ID: "a", Protocol: "socks5"})
	cfg.SetChain(&config.ChainSpec{ID: "member", Layers: []string{"a"}})
	// Realistic config bulk: the pools guard's reverse scan walks every
	// chain, so filler chains widen its scan→commit window and make the
	// pre-fix overlap reliably observable instead of scheduler luck. They
	// reference "a", never X, so they cannot trip either guard themselves.
	for i := 0; i < 128; i++ {
		cfg.SetChain(&config.ChainSpec{ID: fmt.Sprintf("filler-%d", i), Layers: []string{"a"}})
	}

	const rounds, writers = 150, 40
	var conversions, chainWrites atomic.Int64
	for round := 0; round < rounds; round++ {
		// Fresh plain X every round: both directions are independently legal
		// from this state, so only serialization decides which one commits.
		cfg.SetPool(&config.Pool{ID: "X", OutboundIDs: []string{"a"}})
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// Tiny deterministic stagger: same-slot pool/chain writers
				// start together and their scan→commit windows overlap.
				time.Sleep(time.Duration(i%7) * time.Millisecond)
				if i%2 == 0 {
					if stressDo(t, hs, "PUT", "/api/pools/X", map[string]any{
						"chain_ids": []string{"member"},
					}) == http.StatusCreated {
						conversions.Add(1)
					}
				} else {
					if stressDo(t, hs, "POST", "/api/chains", map[string]any{
						"id": "U", "layers": []string{"X"},
					}) == http.StatusCreated {
						chainWrites.Add(1)
					}
				}
			}(i)
		}
		wg.Wait()
		if pathPoolLayerViolation(cfg, "X", "U") {
			t.Fatalf("round %d: pool X became a path-pool while chain U traverses it — concurrent writes defeated the guards", round)
		}
		// Reset for the next round; 404 is fine (the losing direction never
		// created its resource).
		code, _ := do(t, hs, "DELETE", "/api/chains/U", "", nil)
		require.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, code)
		code, _ = do(t, hs, "DELETE", "/api/pools/X", "", nil)
		require.Equal(t, http.StatusNoContent, code)
	}
	// Non-vacuity: both commit paths must actually have run, or the stress
	// proved nothing (e.g. every request errored).
	assert.Positive(t, conversions.Load(), "no pool conversion ever committed")
	assert.Positive(t, chainWrites.Load(), "no chain create ever committed")
}

// TestWriteOrderPoolApplyVsChainCreate pins the same guarantee for the
// pool-mode apply: fullProbeApplyPool must hold writeMu from its
// chainLayerReferencer scan through SetPool, or a chain naming pool_id
// created between scan and write lands the forbidden state. Each round fires
// staggered applies (pool_id X over the canned job's proved paths) against
// chain U creates with layers [X]; never may both commit.
func TestWriteOrderPoolApplyVsChainCreate(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, cfg, _ := newFullProbeServer(t, fr)

	const rounds, writers = 60, 20
	var applies, chainWrites atomic.Int64
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(i%7) * time.Millisecond)
				if i%2 == 0 {
					if stressDo(t, hs, "POST", "/api/probes/full/fp-test/inbounds", map[string]any{
						"mode": "pool", "pool_id": "X", "id_prefix": "ap",
					}) == http.StatusCreated {
						applies.Add(1)
					}
				} else {
					if stressDo(t, hs, "POST", "/api/chains", map[string]any{
						"id": "U", "layers": []string{"X"},
					}) == http.StatusCreated {
						chainWrites.Add(1)
					}
				}
			}(i)
		}
		wg.Wait()
		if pathPoolLayerViolation(cfg, "X", "U") {
			t.Fatalf("round %d: apply created path-pool X while chain U traverses it — concurrent writes defeated the guards", round)
		}
		// Delete everything either direction may have created so the next
		// round starts from the same empty state; 404s are the losing side.
		for _, path := range []string{
			"/api/chains/U", "/api/chains/ap-d1-1", "/api/chains/ap-d2-2",
			"/api/pools/X", "/api/inbounds/ap",
		} {
			code, _ := do(t, hs, "DELETE", path, "", nil)
			require.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, code, path)
		}
	}
	assert.Positive(t, applies.Load(), "no pool-mode apply ever committed")
	assert.Positive(t, chainWrites.Load(), "no chain create ever committed")
}
