package geo

import (
	"testing"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubResolver returns a fixed location keyed by country.
type stubResolver struct{}

func (stubResolver) Lookup(address string) (*core.Location, error) {
	return &core.Location{Country: "US", Province: "CA", City: "SF", Raw: address}, nil
}

func TestManagerCaches(t *testing.T) {
	m := NewManager(stubResolver{})
	loc1, err := m.LookupAddress("1.2.3.4:80")
	require.NoError(t, err)
	assert.Equal(t, "US", loc1.Country)
	// second lookup hits the cache (same pointer)
	loc2, _ := m.LookupAddress("1.2.3.4:80")
	assert.Same(t, loc1, loc2)
}

func TestManagerSplitsHostPort(t *testing.T) {
	m := NewManager(stubResolver{})
	loc, err := m.LookupAddress("example.com:443")
	require.NoError(t, err)
	assert.Equal(t, "example.com", loc.Raw, "host part is passed to resolver")
}

func TestNoopResolver(t *testing.T) {
	m := NewManager(Noop())
	loc, err := m.LookupAddress("1.2.3.4:80")
	require.NoError(t, err)
	assert.Equal(t, "", loc.Country)
}

func TestSetResolverClearsCache(t *testing.T) {
	m := NewManager(stubResolver{})
	m.LookupAddress("1.2.3.4:80")
	m.SetResolver(Noop())
	loc, _ := m.LookupAddress("1.2.3.4:80")
	assert.Equal(t, "", loc.Country, "cache should be cleared after resolver swap")
}
