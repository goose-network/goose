// Package config defines the engine's configuration schema and a live store
// that supports hot, concurrency-safe updates from the RESTful API. The
// store is the single source of truth for inbounds, pools, outbounds and
// chains; the engine subscribes to changes and reconciles its runtime
// objects (listeners, pools) accordingly.
package config

import (
	"encoding/json"
	"os"
	"sync"
	"time"
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
	// ExitChecks configures scheduled exit-IP checks: the engine dials each
	// outbound and asks an "what is my IP" style site for the exit IP,
	// recording results. Zero value (disabled) keeps the engine from making
	// any unsolicited outbound requests.
	ExitChecks ExitCheckConfig `json:"exit_checks"`
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
	// Policy overrides the inbound-level policy for this user. Route fields
	// (chain_id / outbound_id / pool_id) are replaced as a group when the
	// user policy sets any of them, otherwise the inbound's route is
	// inherited. Filters are the user's own: when present they replace the
	// inbound-level filters, so each user can be narrowed independently.
	Policy *InboundPolicy `json:"policy,omitempty"`
}

// InboundPolicy binds a route to an inbound or user. A route is one of:
//
//   - ChainID: a named chain (ordered pool/outbound layers) from the chain
//     store;
//   - OutboundID: one specific outbound server, pinned;
//   - PoolID + Filters: a single pool layer, with optional filters that
//     further narrow the pool;
//   - none of them: the engine falls back to picking any available outbound.
//
// Resolution order when several are set: ChainID, then OutboundID, then
// PoolID; an unresolvable reference (e.g. a missing chain) falls through to
// the next. Filters apply no matter which route resolved: they narrow every
// layer of a named chain as well as a pool's candidates.
type InboundPolicy struct {
	// ChainID references a Chain in the chain store.
	ChainID string `json:"chain_id"`
	// OutboundID references one Outbound to route through directly,
	// pinning this inbound (or user) to a single server. Takes precedence
	// over PoolID, but not over ChainID.
	OutboundID string `json:"outbound_id"`
	// PoolID references a Pool to route through directly, without needing a
	// named chain. Ignored when ChainID or OutboundID is set.
	PoolID string `json:"pool_id"`
	// Filters narrow the route's candidates for this inbound or user only
	// (same FilterSpec shape as a pool's filters). With pool routing they
	// apply on top of the pool's own filters; with chain routing they narrow
	// every layer's pool. A user's filters replace the inbound-level ones.
	Filters []FilterSpec `json:"filters"`
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
	ID          string       `json:"id"`
	OutboundIDs []string     `json:"outbound_ids"`
	Filters     []FilterSpec `json:"filters"`
	Selector    SelectorSpec `json:"selector"`
	// Access, when set, turns the pool into an auto-chained pool: the engine
	// probes each member's server for direct reachability from the local
	// network; members the local ISP cannot reach are dialed through an
	// access proxy from Access.AccessPoolID (a relay hop is prepended
	// automatically), and members reachable neither directly nor through the
	// access pool are dropped from the pool's candidates.
	Access *AccessFilterSpec `json:"access,omitempty"`
}

// AccessFilterSpec configures automatic proxy-chain building for a pool.
type AccessFilterSpec struct {
	// AccessPoolID is the pool of access (relay) proxies used to reach
	// members whose servers the local network cannot dial directly (e.g.
	// proxies behind an ISP block). Required; must reference an existing pool.
	AccessPoolID string `json:"access_pool_id"`
	// ProbeIntervalSec is seconds between reachability probe rounds.
	// Default 300; minimum 10.
	ProbeIntervalSec int `json:"probe_interval_seconds"`
	// ProbeTimeoutSec bounds each probe dial. Default 5; minimum 1.
	ProbeTimeoutSec int `json:"probe_timeout_seconds"`
}

// EffectiveProbeIntervalSec returns the probe interval with the default
// applied and the floor enforced.
func (a *AccessFilterSpec) EffectiveProbeIntervalSec() int {
	if a.ProbeIntervalSec < 10 {
		return 300
	}
	return a.ProbeIntervalSec
}

// EffectiveProbeTimeoutSec returns the probe dial timeout with the default
// applied and the floor enforced.
func (a *AccessFilterSpec) EffectiveProbeTimeoutSec() int {
	if a.ProbeTimeoutSec < 1 {
		return 5
	}
	return a.ProbeTimeoutSec
}

// ExitCheckConfig configures the engine's exit-IP check subsystem: for each
// configured outbound, dial through it to an "what is my IP" style site and
// record the observed exit IP. A proxy's connect address is not its exit
// address — the exit IP can vary by target site and over time — so checks
// are recorded per (outbound, site) with timestamps.
type ExitCheckConfig struct {
	// Enabled turns scheduled checks on. Default false.
	Enabled bool `json:"enabled"`
	// Cron is a 5-field crontab expression (min hour dom mon dow) for the
	// check schedule. When set it takes precedence over IntervalSec.
	Cron string `json:"cron"`
	// IntervalSec is seconds between check rounds, used when Cron is empty.
	IntervalSec int `json:"interval_seconds"`
	// Sites are exit-IP check URLs that respond with the caller's IP as
	// plain text (e.g. https://ifconfig.me). Checks rotate round-robin
	// across this list so large batches spread load instead of tripping one
	// site's rate limits. Empty selects the built-in defaults.
	Sites []string `json:"sites"`
	// TimeoutSec bounds each check. Default 15; minimum 1.
	TimeoutSec int `json:"timeout_seconds"`
	// Concurrency caps parallel checks in one round, protecting the check
	// sites (and their rate limits) from a burst. Default 4; minimum 1.
	Concurrency int `json:"concurrency"`
}

// EffectiveTimeoutSec returns the per-check timeout with the default
// applied and the floor enforced.
func (c *ExitCheckConfig) EffectiveTimeoutSec() time.Duration {
	if c.TimeoutSec < 1 {
		return 15 * time.Second
	}
	return time.Duration(c.TimeoutSec) * time.Second
}

// EffectiveConcurrency returns the parallel-check cap with the default
// applied and the floor enforced.
func (c *ExitCheckConfig) EffectiveConcurrency() int {
	if c.Concurrency < 1 {
		return 4
	}
	return c.Concurrency
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

// ChainSpec is the config form of a proxy chain: an ordered list of layers.
// A layer references either a pool (which carries its own filters +
// selector) or a single outbound server (pinned for that hop). When an ID
// matches both a pool and an outbound, the pool wins.
type ChainSpec struct {
	ID     string   `json:"id"`
	Layers []string `json:"layers"` // pool or outbound IDs, in order
}

// DefaultPoolID is the managed pool providers populate when their spec does
// not name one, so a provider can be created with just a URL and still land
// somewhere routable.
const DefaultPoolID = "default"

// ProviderSpec configures a dynamic outbound provider: a plugin that sources
// its own proxy server configs and feeds them into managed pools. The engine
// instantiates the provider from (Provider, Config), polls Outbounds, and
// merges the result into the store as managed outbounds plus one pool per
// listed PoolID. This is how a plugin like Psiphon (which fetches and
// refreshes a remote server list) gets its servers into the engine's pools.
//
// A provider may feed several pools at once — the same outbound set lands in
// each, so e.g. one subscription can back both a "fast" pool and a "fallback"
// pool without duplicating the fetch. The legacy singular `pool_id` JSON
// field is still accepted and is just a one-entry PoolIDs.
type ProviderSpec struct {
	ID       string `json:"id"`
	Provider string `json:"provider"` // plugin name, e.g. "psiphon"
	// PoolIDs are the managed pools to populate; empty means the default pool.
	PoolIDs []string       `json:"pool_ids,omitempty"`
	Config  map[string]any `json:"config"`
}

// UnmarshalJSON accepts both the plural `pool_ids` array and the legacy
// singular `pool_id` string, so existing configs and API clients keep working.
func (p *ProviderSpec) UnmarshalJSON(b []byte) error {
	type plain ProviderSpec // avoid recursing into this method
	var raw struct {
		plain
		PoolID  string   `json:"pool_id"`
		PoolIDs []string `json:"pool_ids"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*p = ProviderSpec(raw.plain)
	ids := raw.PoolIDs
	if raw.PoolID != "" {
		ids = append([]string{raw.PoolID}, ids...)
	}
	p.PoolIDs = ids
	return nil
}

// MarshalJSON emits only the canonical `pool_ids` form; the singular
// `pool_id` stays a read-compat input, not a second source of truth.
func (p ProviderSpec) MarshalJSON() ([]byte, error) {
	type plain ProviderSpec
	out := struct {
		plain
		PoolIDs []string `json:"pool_ids"`
	}{plain: plain(p), PoolIDs: p.PoolIDs}
	// An explicit (possibly empty) pool_ids so the field always shows up in
	// API responses, rather than omitempty dropping it.
	return json.Marshal(out)
}

// EffectivePoolIDs returns the pools this spec's outbounds merge into,
// applying the default-pool rule for specs created without any pool:
// PoolIDs with empty entries dropped, duplicates removed, order preserved.
func (p *ProviderSpec) EffectivePoolIDs() []string {
	out := make([]string, 0, len(p.PoolIDs)+1)
	seen := map[string]struct{}{}
	for _, id := range p.PoolIDs {
		if id == "" {
			id = DefaultPoolID
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		out = append(out, DefaultPoolID)
	}
	return out
}

// Store is the concurrency-safe live configuration store.
type Store struct {
	mu        sync.RWMutex
	engine    Engine
	inbounds  map[string]*Inbound
	outbounds map[string]*OutboundSpec
	pools     map[string]*Pool
	chains    map[string]*ChainSpec
	providers map[string]*ProviderSpec
	// managedOutbounds records outbound IDs owned by a provider, so
	// SetProviderOutbounds can replace a provider's full set atomically
	// (adding new, removing stale) without touching user-configured static
	// outbounds. Keyed by provider ID.
	managedOutbounds map[string]map[string]struct{}
	// version is bumped on every mutation; subscribers use it to detect
	// changes and reconcile.
	version uint64
	subs    []chan uint64
}

// NewStore returns an empty store with sensible defaults.
func NewStore() *Store {
	// GOOSE_API_LISTEN overrides the default admin API address for
	// containerized runs: the built-in 127.0.0.1 default binds the
	// container's own loopback, unreachable through a published port.
	// An explicit config file still wins — loadConfig applies it after
	// NewStore and replaces the engine config wholesale.
	listen := "127.0.0.1:9090"
	if v := os.Getenv("GOOSE_API_LISTEN"); v != "" {
		listen = v
	}
	return &Store{
		engine:           Engine{Stack: "system", API: APIConfig{Listen: listen}, DB: "goose.db"},
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
	spec, ok := s.providers[id]
	if !ok {
		return false
	}
	delete(s.providers, id)
	// also drop any managed outbounds + pools this provider owned
	s.clearManagedLocked(id)
	// The managed pools are keyed by the spec's effective pool IDs, not the
	// provider id. A pool may also be fed by OTHER providers (shared pool):
	// only delete it when no remaining provider still targets it, and
	// rebuild the survivors' view instead of leaving stale outbound refs.
	for _, poolID := range spec.EffectivePoolIDs() {
		if !s.providerFeedsPoolLocked(poolID) {
			delete(s.pools, poolID)
			continue
		}
		s.rebuildProviderPoolLocked(poolID)
	}
	s.bumpLocked()
	return true
}

// providerFeedsPoolLocked reports whether any remaining provider targets
// poolID. Caller holds s.mu.
func (s *Store) providerFeedsPoolLocked(poolID string) bool {
	for _, spec := range s.providers {
		for _, id := range spec.EffectivePoolIDs() {
			if id == poolID {
				return true
			}
		}
	}
	return false
}

// SetProviderOutbounds replaces the full set of outbounds owned by providerID
// with specs, and (re)builds every managed pool the provider feeds to
// reference them. This is the "get proxy server config from plugin into
// pool" seam: the engine calls it with the result of Provider.Outbounds. It
// bumps the config version exactly once so the router rebuilds with the new
// pools.
//
// The provider's pools are its spec's effective pool IDs (multi-pool): the
// whole outbound set lands in EACH pool, merged with every other provider
// targeting the same pool. Outbounds whose IDs already exist (from any
// source) are overwritten; the provider takes ownership of the given IDs.
// Previously-owned IDs not present in specs are removed (but only if still
// owned by this provider, so a user re-claiming an ID by hand is respected).
// Pools are created if missing. Pools the provider used to feed but no
// longer does (its pool list was edited) are rebuilt without it — or deleted
// when nothing else feeds them.
func (s *Store) SetProviderOutbounds(providerID string, specs []*OutboundSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A deleted provider's late poll result is dropped: the manager cancels
	// its poll loop on delete, but an in-flight Outbounds call can complete
	// after the delete already cleared the provider's outbounds and pools.
	// Merging then would resurrect them.
	provSpec, ok := s.providers[providerID]
	if !ok {
		return
	}
	poolIDs := provSpec.EffectivePoolIDs()

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

	// (re)build every pool the spec targets. rebuildProviderPoolLocked
	// unions in every OTHER provider still aimed at the same pool, so two
	// providers sharing a pool (e.g. two subscriptions both landing in
	// "default") accumulate instead of clobbering each other on each poll.
	fed := make(map[string]struct{}, len(poolIDs))
	for _, poolID := range poolIDs {
		fed[poolID] = struct{}{}
		s.rebuildProviderPoolLocked(poolID)
	}
	// Pools that reference this provider's outbounds but are no longer in
	// its pool list: rebuild without it, or delete when unfed.
	for _, poolID := range s.fedPoolsLocked(providerID) {
		if _, keep := fed[poolID]; keep {
			continue
		}
		if !s.providerFeedsPoolLocked(poolID) {
			delete(s.pools, poolID)
			continue
		}
		s.rebuildProviderPoolLocked(poolID)
	}

	s.bumpLocked()
}

// fedPoolsLocked returns the pools that reference any of providerID's managed
// outbounds — the pools it actually feeds even if the spec was just edited.
// Caller holds s.mu.
func (s *Store) fedPoolsLocked(providerID string) []string {
	owned := s.managedOutbounds[providerID]
	var pools []string
	for poolID, pool := range s.pools {
		for _, id := range pool.OutboundIDs {
			if _, mine := owned[id]; mine {
				pools = append(pools, poolID)
				break
			}
		}
	}
	return pools
}

// rebuildProviderPoolLocked recomputes pool poolID as the union of every
// provider's managed outbounds whose spec still targets it, preserving any
// user-configured Filters/Selector/Access. Callers hold s.mu. The pool is
// replaced by a fresh *Pool rather than mutated in place: the store hands
// *Pool pointers to readers (Pool()/Pools()), which hold them beyond the
// lock — mutating pool.OutboundIDs in place races with those readers.
// Publishing a fresh Pool keeps every previously-returned pointer an
// immutable snapshot.
func (s *Store) rebuildProviderPoolLocked(poolID string) {
	ids := []string{}
	for providerID, owned := range s.managedOutbounds {
		// Only providers still aimed at this pool contribute; a provider
		// that moved to another pool leaves its old IDs behind.
		spec, ok := s.providers[providerID]
		if !ok {
			continue
		}
		targets := false
		for _, id := range spec.EffectivePoolIDs() {
			if id == poolID {
				targets = true
				break
			}
		}
		if !targets {
			continue
		}
		for id := range owned {
			if _, ok := s.outbounds[id]; !ok {
				continue
			}
			ids = append(ids, id)
		}
	}
	pool := &Pool{
		ID:          poolID,
		OutboundIDs: ids,
	}
	if existing, ok := s.pools[poolID]; ok {
		// keep user-configured filters/selector/access; if the pool is
		// brand-new and has no selector, the router falls back to a
		// round-robin-style fallback selector.
		pool.Filters = existing.Filters
		pool.Selector = existing.Selector
		pool.Access = existing.Access
	}
	s.pools[poolID] = pool
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
