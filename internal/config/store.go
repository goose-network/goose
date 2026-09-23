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

// Store is the concurrency-safe live configuration store.
type Store struct {
	mu       sync.RWMutex
	engine   Engine
	inbounds map[string]*Inbound
	outbounds map[string]*OutboundSpec
	pools    map[string]*Pool
	chains   map[string]*ChainSpec
	// version is bumped on every mutation; subscribers use it to detect
	// changes and reconcile.
	version  uint64
	subs     []chan uint64
}

// NewStore returns an empty store with sensible defaults.
func NewStore() *Store {
	return &Store{
		engine:    Engine{Stack: "system", API: APIConfig{Listen: "127.0.0.1:9090"}, DB: "goose.db"},
		inbounds:  map[string]*Inbound{},
		outbounds: map[string]*OutboundSpec{},
		pools:     map[string]*Pool{},
		chains:    map[string]*ChainSpec{},
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
