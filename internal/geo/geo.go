// Package geo resolves the geographic/network location of an outbound from
// its dial address, using an offline IP database (nali-style: MMDB, qqwry,
// zxinc). The engine calls Lookup when an outbound is imported so that
// location-based filters and selectors work without any network calls.
package geo

import (
	"fmt"
	"net"
	"sync"

	"github.com/goose-network/goose/internal/core"
)

// Resolver looks up the Location for a host:port or IP string.
type Resolver interface {
	Lookup(address string) (*core.Location, error)
}

// noopResolver returns an empty location. Used when geo is disabled.
type noopResolver struct{}

func (noopResolver) Lookup(string) (*core.Location, error) {
	return &core.Location{}, nil
}

// Noop returns a resolver that resolves everything to an empty location.
func Noop() Resolver { return noopResolver{} }

// Manager owns the active resolver and caches lookups by IP.
type Manager struct {
	mu    sync.RWMutex
	r     Resolver
	cache map[string]*core.Location
}

// NewManager wraps a resolver with an in-memory cache. Pass Noop() to
// disable geo-location.
func NewManager(r Resolver) *Manager {
	if r == nil {
		r = Noop()
	}
	return &Manager{r: r, cache: map[string]*core.Location{}}
}

// LookupAddress resolves the host part of a host:port address. If the host
// is a domain, it is not resolved to an IP here (the outbound's real exit
// IP is what matters for geo); callers should pass the outbound's server IP
// when known. For domains, the cache key is the domain itself.
func (m *Manager) LookupAddress(address string) (*core.Location, error) {
	host := address
	if h, _, err := net.SplitHostPort(address); err == nil {
		host = h
	}
	m.mu.RLock()
	if loc, ok := m.cache[host]; ok {
		m.mu.RUnlock()
		return loc, nil
	}
	m.mu.RUnlock()
	loc, err := m.r.Lookup(host)
	if err != nil {
		return nil, fmt.Errorf("geo: lookup %s: %w", host, err)
	}
	m.mu.Lock()
	m.cache[host] = loc
	m.mu.Unlock()
	return loc, nil
}

// SetResolver swaps the active resolver (e.g. after a config change) and
// clears the cache.
func (m *Manager) SetResolver(r Resolver) {
	if r == nil {
		r = Noop()
	}
	m.mu.Lock()
	m.r = r
	m.cache = map[string]*core.Location{}
	m.mu.Unlock()
}
