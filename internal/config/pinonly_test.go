package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPinOnlyExcludedFromAutoPoolMembership verifies a provider's pin-only
// outbounds stay advertised (resolvable for explicit pins) but are kept out of
// the provider-managed auto-pool, so tier-unusable per-server outbounds never
// enter default-pool rotation. An operator-built pool that names a pin-only
// outbound explicitly is a separate path and still honors it.
func TestPinOnlyExcludedFromAutoPoolMembership(t *testing.T) {
	s := NewStore()
	s.SetProvider(&ProviderSpec{ID: "geph", Provider: "geph"}) // no PoolIDs -> default
	s.SetProviderOutbounds("geph", []*OutboundSpec{
		{ID: "geph", Protocol: "geph", Config: map[string]any{}},
		{ID: "geph-plus1", Protocol: "geph", Config: map[string]any{"pin_only": true}},
	})

	p, ok := s.Pool("default")
	require.True(t, ok, "default pool should be built from provider output")
	ids := map[string]bool{}
	for _, id := range p.OutboundIDs {
		ids[id] = true
	}
	assert.False(t, ids["geph-plus1"], "pin-only outbound must be excluded from auto-pool membership")
	assert.True(t, ids["geph"], "auto-selectable outbound must be in the default pool")

	// The pin-only outbound is still advertised, so an explicit chain/pool ref resolves it.
	_, ok = s.Outbound("geph-plus1")
	assert.True(t, ok, "pin-only outbound must remain advertised for explicit references")

	// An operator-built pool naming the pin-only outbound explicitly still honors it.
	s.SetPool(&Pool{ID: "manual", OutboundIDs: []string{"geph-plus1"}})
	mp, ok := s.Pool("manual")
	require.True(t, ok)
	assert.Equal(t, []string{"geph-plus1"}, mp.OutboundIDs, "explicit operator pool must honor a pin-only outbound")
}

// TestPinOnlyHelper reads the reserved config key robustly: absent or non-bool
// values are not pin-only; only an explicit true marks it.
func TestPinOnlyHelper(t *testing.T) {
	assert.False(t, (&OutboundSpec{Config: map[string]any{}}).PinOnly())
	assert.False(t, (&OutboundSpec{Config: map[string]any{"pin_only": "true"}}).PinOnly())
	assert.False(t, (&OutboundSpec{Config: map[string]any{"pin_only": false}}).PinOnly())
	assert.True(t, (&OutboundSpec{Config: map[string]any{"pin_only": true}}).PinOnly())
}