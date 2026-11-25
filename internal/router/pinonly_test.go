package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
)

// TestPinOnlyExcludedFromFallbackButResolvesAsChainRef verifies the engine half of
// the pin-only contract: a pin-only outbound is skipped by the implicit
// all-outbounds fallback (so a provider's tier-unusable per-server outbounds never
// enter rotation), yet still resolves as an explicit chain layer — the H2
// silent-truncation regression guard.
func TestPinOnlyExcludedFromFallbackButResolvesAsChainRef(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "auto", Protocol: "testdirect", Config: map[string]any{"id": "auto"}},
			{ID: "pinned", Protocol: "testdirect", Config: map[string]any{"id": "pinned", "pin_only": true}},
		},
		nil,
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"pinned"}}},
		nil,
	)

	// The implicit fallback (a layer with no pool selector) rotates over
	// auto-selectable outbounds only.
	ids := map[string]bool{}
	for _, o := range r.layerPool(core.ChainLayer{}) {
		ids[o.ID()] = true
	}
	assert.False(t, ids["pinned"], "pin-only outbound must be excluded from the implicit fallback")
	assert.True(t, ids["auto"], "auto-selectable outbound must be in the implicit fallback")

	// An explicit chain reference still resolves the pin-only outbound.
	r.mu.RLock()
	ch := r.chains["c1"]
	r.mu.RUnlock()
	require.Len(t, ch, 1, "chain pinned to a pin-only outbound must resolve, not silently truncate")
}