// Package config defines the engine's configuration schema and a live store
// that supports hot, concurrency-safe updates from the RESTful API. The
// store is the single source of truth for inbounds, pools, outbounds and
// chains; the engine subscribes to changes and reconciles its runtime
// objects (listeners, pools) accordingly.
package config

import (
	"sync"
)

// Engine is the top-level configuration.
type Engine struct {
	// Stack selects the network stack: "system" or "gvisor".
	Stack string `json:"stack"`
	// API is the admin server config.
	API APIConfig `json:"api"`
	// DB is the metrics database path.
	DB string `json:"db"`
	// Geo is the offline IP database config.
	Geo GeoConfig `json:"geo"`
}

type APIConfig struct {
	Listen string `json:"listen"`
	// Token is the bearer token for admin auth; empty disables auth.
	Token string `json:"token"`
}

type GeoConfig struct {
	// Type is "mmdb", "qqwry", "zxinc" or "" (disabled).
	Type string `json:"type"`
	// Path is the database file path.
	Path string `json:"path"`
}

// Inbound describes a listener. One port may carry multiple users; each
// (port, user) pair resolves to an InboundPolicy.
type Inbound struct {
	ID       string        `json:"id"`
	Protocol string        `json:"protocol"` // "http" | "socks5"
	Listen   string        `json:"listen"`   // host:port
	Users    []User        `json:"users"`    // empty => no auth
	Policy   InboundPolicy `json:"policy"`
}

// User is one authenticated inbound user. A port with multiple users
// dispatches each connection to that user's policy.
type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// Policy overrides the inbound-level policy for this user. Empty fields
	// fall back to the inbound-level policy.
	Policy *InboundPolicy `json:"policy,omitempty"`
}

// InboundPolicy binds a chain (and thus its filters + LB strategy) to an
// inbound or user.
type InboundPolicy struct {
	// ChainID references a Chain in the chain store.
	ChainID string `json:"chain_id"`
}

// OutboundSpec is the config form of an outbound, before it is built into a
// core.Outbound by the plugin factory.
type OutboundSpec struct {
	ID       string         `json:"id"`
	Protocol string         `json:"protocol"`
	Config   map[string]any `json:"config"`
}

// Pool is a named, ordered set of outbound IDs plus the filters and
// selection strategy applied when picking from it.
type Pool struct {
	ID         string        `json:"id"`
	OutboundIDs []string     `json:"outbound_ids"`
	Filters    []FilterSpec  `json:"filters"`
	Selector   SelectorSpec  `json:"selector"`
}

// FilterSpec configures one filter dimension.
type FilterSpec struct {
	// Type is "location", "latency", "protocol".
	Type string `json:"type"`
	// Params is filter-specific config, e.g. {"countries":["US"]}.
	Params map[string]any `json:"params"`
}

// SelectorSpec configures the load-balance strategy.
type SelectorSpec struct {
	// Type is "random", "roundrobin", "sticky", "leastlatency".
	Type string `json:"type"`
	// Params is strategy-specific config, e.g. sticky: {"key":"domain"}.
	Params map[string]any `json:"params"`
}

// ChainSpec is the config form of a proxy chain: an ordered list of layers,
// each referencing a pool (which carries its own filters + selector).
type ChainSpec struct {
	ID     string   `json:"id"`
	Layers []string `json:"layers"` // PoolIDs, in order
}

// ProviderSpec configures a dynamic outbound provider: a plugin that sources
// its own proxy server configs and feeds them into a managed pool. The engine
// instantiates the provider from (Provider, Config), polls Outbounds, and
// merges the result into the store as managed outbounds plus a pool named
// PoolID. This is how a plugin like Psiphon (which fetches and refreshes a
// remote server list) gets its servers into the engine's pool.
type ProviderSpec struct {
	ID       string         `json:"id"`
	Provider string         `json:"provider"` // plugin name, e.g. "psiphon"
	PoolID   string         `json:"pool_id"`  // managed pool to populate
	Config   map[string]any `json:"config"`
}

// Store is the concurrency-safe live configuration store.
type Store struct {
	mu       sync.RWMutex
	engine   Engine
	inbounds map[string]*Inbound
	outbounds map[string]*OutboundSpec
	pools    map[string]*Pool
	chains   map[string]*ChainSpec
	providers map[string]*ProviderSpec
	// managedOutbounds records outbound IDs owned by a provider, so
	// SetProviderOutbounds can replace a provider's full set atomically
	// (adding new, removing stale) without touching user-configured static
	// outbounds. Keyed by provider ID.
	managedOutbounds map[string]map[string]struct{}
	// version is bumped on every mutation; subscribers use it to detect
	// changes and reconcile.
	version  uint64
	subs     []chan uint64
}

// NewStore returns an empty store with sensible defaults.
func NewStore() *Store {
	return &Store{
		engine:           Engine{Stack: "system", API: APIConfig{Listen: "127.0.0.1:9090"}, DB: "goose.db"},
		inbounds:         map[string]*Inbound{},
		outbounds:        map[string]*OutboundSpec{},
		pools:            map[string]*Pool{},
		chains:           map[string]*ChainSpec{},
		providers:        map[string]*ProviderSpec{},
		managedOutbounds: map[string]map[string]struct{}{},
	}
}

// Engine returns the engine-level config snapshot.
func (s *Store) Engine() Engine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engine
}

// SetEngine replaces the engine-level config.
func (s *Store) SetEngine(e Engine) {
	s.mu.Lock()
	s.engine = e
	s.bumpLocked()
	s.mu.Unlock()
}

// --- Inbounds ---

func (s *Store) Inbounds() []*Inbound {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Inbound, 0, len(s.inbounds))
	for _, v := range s.inbounds {
		out = append(out, v)
	}
	return out
}
func (s *Store) Inbound(id string) (*Inbound, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.inbounds[id]
	return v, ok
}
func (s *Store) SetInbound(in *Inbound) {
	s.mu.Lock()
	s.inbounds[in.ID] = in
	s.bumpLocked()
	s.mu.Unlock()
}
func (s *Store) DeleteInbound(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inbounds[id]; !ok {
		return false
	}
	delete(s.inbounds, id)
	s.bumpLocked()
	return true
}

// --- Outbounds ---

func (s *Store) Outbounds() []*OutboundSpec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*OutboundSpec, 0, len(s.outbounds))
	for _, v := range s.outbounds {
		out = append(out, v)
	}
	return out
}
func (s *Store) Outbound(id string) (*OutboundSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.outbounds[id]
	return v, ok
}
func (s *Store) SetOutbound(o *OutboundSpec) {
	s.mu.Lock()
	s.outbounds[o.ID] = o
	s.bumpLocked()
	s.mu.Unlock()
}
func (s *Store) DeleteOutbound(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.outbounds[id]; !ok {
		return false
	}
	delete(s.outbounds, id)
	s.bumpLocked()
	return true
}

// --- Pools ---

func (s *Store) Pools() []*Pool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Pool, 0, len(s.pools))
	for _, v := range s.pools {
		out = append(out, v)
	}
	return out
}
func (s *Store) Pool(id string) (*Pool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.pools[id]
	return v, ok
}
func (s *Store) SetPool(p *Pool) {
	s.mu.Lock()
	s.pools[p.ID] = p
	s.bumpLocked()
	s.mu.Unlock()
}
func (s *Store) DeletePool(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pools[id]; !ok {
		return false
	}
	delete(s.pools, id)
	s.bumpLocked()
	return true
}

// --- Chains ---

func (s *Store) Chains() []*ChainSpec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ChainSpec, 0, len(s.chains))
	for _, v := range s.chains {
		out = append(out, v)
	}
	return out
}
func (s *Store) Chain(id string) (*ChainSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.chains[id]
	return v, ok
}
func (s *Store) SetChain(c *ChainSpec) {
	s.mu.Lock()
	s.chains[c.ID] = c
	s.bumpLocked()
	s.mu.Unlock()
}
func (s *Store) DeleteChain(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chains[id]; !ok {
		return false
	}
	delete(s.chains, id)
	s.bumpLocked()
	return true
}

// --- Providers ---

func (s *Store) Providers() []*ProviderSpec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ProviderSpec, 0, len(s.providers))
	for _, v := range s.providers {
		out = append(out, v)
	}
	return out
}
func (s *Store) Provider(id string) (*ProviderSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.providers[id]
	return v, ok
}
func (s *Store) SetProvider(p *ProviderSpec) {
	s.mu.Lock()
	s.providers[p.ID] = p
	s.bumpLocked()
	s.mu.Unlock()
}
func (s *Store) DeleteProvider(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.providers[id]; !ok {
		return false
	}
	delete(s.providers, id)
	// also drop any managed outbounds + pool this provider owned
	s.clearManagedLocked(id)
	delete(s.pools, id)
	s.bumpLocked()
	return true
}

// SetProviderOutbounds replaces the full set of outbounds owned by providerID
// with specs, and (re)builds the managed pool poolID to reference them. This
// is the "get proxy server config from plugin into pool" seam: the engine
// calls it with the result of Provider.Outbounds. It bumps the config version
// exactly once so the router rebuilds with the new pool.
//
// Outbounds whose IDs already exist (from any source) are overwritten; the
// provider takes ownership of the given IDs. Previously-owned IDs not present
// in specs are removed (but only if still owned by this provider, so a user
// re-claiming an ID by hand is respected). The pool is created if missing.
func (s *Store) SetProviderOutbounds(providerID, poolID string, specs []*OutboundSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.managedOutbounds[providerID]
	next := make(map[string]struct{}, len(specs))

	// add/update each spec
	for _, spec := range specs {
		s.outbounds[spec.ID] = spec
		next[spec.ID] = struct{}{}
	}
	// remove stale outbounds previously owned by this provider
	for id := range prev {
		if _, keep := next[id]; keep {
			continue
		}
		delete(s.outbounds, id)
	}
	s.managedOutbounds[providerID] = next

	// (re)build the managed pool to reference exactly these outbounds.
	ids := make([]string, 0, len(specs))
	for _, spec := range specs {
		ids = append(ids, spec.ID)
	}
	pool, ok := s.pools[poolID]
	if !ok {
		pool = &Pool{ID: poolID}
		s.pools[poolID] = pool
	}
	pool.OutboundIDs = ids
	// leave any user-configured filters/selector on the pool intact; if the
	// pool is brand-new and has no selector, the router falls back to a
	// round-robin-style fallback selector.

	s.bumpLocked()
}

// clearManagedLocked drops all outbounds owned by providerID. Caller holds s.mu.
func (s *Store) clearManagedLocked(providerID string) {
	for id := range s.managedOutbounds[providerID] {
		delete(s.outbounds, id)
	}
	delete(s.managedOutbounds, providerID)
}

// --- change subscription ---

// Subscribe returns a channel that receives the new version on every
// mutation. The channel is buffered(1); subscribers must drain promptly.
func (s *Store) Subscribe() <-chan uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan uint64, 1)
	s.subs = append(s.subs, ch)
	return ch
}

// Version returns the current config version.
func (s *Store) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// bumpLocked notifies subscribers of a new version. Caller holds s.mu.
func (s *Store) bumpLocked() {
	s.version++
	ver := s.version
	for _, ch := range s.subs {
		select {
		case ch <- ver:
		default:
			// subscriber is slow; drop. It will see a later version.
		}
	}
}
