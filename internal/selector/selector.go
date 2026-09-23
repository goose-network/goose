// Package selector implements the load-balancing strategies that pick one
// outbound from a pool for a given request. Strategies are stateless w.r.t.
// the pool membership (the router supplies the filtered candidate list) but
// may carry their own selection state (a round-robin counter, a sticky
// cache). All strategies are safe for concurrent use.
package selector

import (
	"errors"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"encoding/binary"

	"github.com/goose-network/goose/internal/core"
)

// ErrEmptyPool is returned when a selector has no candidates to pick from.
var ErrEmptyPool = errors.New("selector: empty pool after filtering")

// New builds a selector from a type name + params, or returns an error.
func New(typ string, params map[string]any) (core.Selector, error) {
	switch typ {
	case "", "random":
		return &Random{}, nil
	case "roundrobin":
		return &RoundRobin{}, nil
	case "sticky":
		return newSticky(params)
	case "leastlatency":
		return newLeastLatency(params)
	default:
		return nil, errors.New("selector: unknown type " + typ)
	}
}

// --- random ---

// Random picks a uniformly random candidate.
type Random struct{}

func (Random) Name() string { return "random" }
func (Random) Pick(pool []core.Outbound, _ core.Metadata) (core.Outbound, error) {
	if len(pool) == 0 {
		return nil, ErrEmptyPool
	}
	// avoid math/rand global lock cost; use a cheap fnv-of-time mix.
	h := fnv.New64a()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(time.Now().UnixNano()))
	h.Write(buf[:])
	idx := int(h.Sum64() % uint64(len(pool)))
	return pool[idx], nil
}

// --- round robin ---

// RoundRobin cycles through candidates in order, skipping the cost of
// per-call randomness.
type RoundRobin struct{ counter uint64 }

func (*RoundRobin) Name() string { return "roundrobin" }
func (r *RoundRobin) Pick(pool []core.Outbound, _ core.Metadata) (core.Outbound, error) {
	if len(pool) == 0 {
		return nil, ErrEmptyPool
	}
	idx := atomic.AddUint64(&r.counter, 1) - 1
	return pool[int(idx%uint64(len(pool)))], nil
}

// --- sticky (session stickiness by destination domain) ---

// stickyKey decides what the stickiness key is for a request.
type stickyKey int

const (
	stickyByDomain stickyKey = iota
	stickyBySrcAndDomain
)

// Sticky maps a request key to a consistent outbound using jump hash. The
// same destination (or source+destination) always lands on the same
// outbound until the pool membership changes, which preserves session
// affinity (e.g. for TLS connections to one origin).
type Sticky struct {
	key  stickyKey
	ttl  time.Duration
	mu   sync.Mutex
	cache map[string]stickyEntry
}

type stickyEntry struct {
	out  core.Outbound
	exp  time.Time
}

func newSticky(params map[string]any) (*Sticky, error) {
	s := &Sticky{cache: map[string]stickyEntry{}, ttl: 10 * time.Minute}
	if v, ok := params["key"].(string); ok && v == "src+domain" {
		s.key = stickyBySrcAndDomain
	}
	if v, ok := params["ttl"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			s.ttl = d
		}
	}
	return s, nil
}

func (s *Sticky) Name() string { return "sticky" }

func (s *Sticky) keyFor(meta core.Metadata) string {
	host := meta.TargetHost
	// use eTLD+1 when possible so related subdomains stick together.
	if d := effectiveTLDPlusOne(host); d != "" {
		host = d
	}
	if s.key == stickyBySrcAndDomain && meta.SourceIP != nil {
		return net.JoinHostPort(meta.SourceIP.String(), host)
	}
	return host
}

func (s *Sticky) Pick(pool []core.Outbound, meta core.Metadata) (core.Outbound, error) {
	if len(pool) == 0 {
		return nil, ErrEmptyPool
	}
	k := s.keyFor(meta)
	if k == "" {
		// fall back to random for keyless requests (e.g. IP targets).
		return Random{}.Pick(pool, meta)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.cache[k]; ok && time.Now().Before(e.exp) && contains(pool, e.out) {
		return e.out, nil
	}
	// jump hash for consistent, minimal-reshuffle placement.
	idx := jumpHash(k, len(pool))
	out := pool[idx]
	s.cache[k] = stickyEntry{out: out, exp: time.Now().Add(s.ttl)}
	return out, nil
}

func contains(pool []core.Outbound, o core.Outbound) bool {
	for _, c := range pool {
		if c.ID() == o.ID() {
			return true
		}
	}
	return false
}

// jumpHash implements Jump Consistent Hash (Lamping & Veach). It distributes
// a uint64 key uniformly across n buckets with minimal movement when n
// changes.
func jumpHash(key string, n int) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	k := int64(h.Sum64())
	var b, j int64 = -1, 0
	for j < int64(n) {
		b = j
		k = k*2862933555777941757 + 1
		// Standard Jump Consistent Hash: j = (b+1) * (2^31 / ((k>>33)+1)).
		j = int64(float64(b+1) * (float64(1<<31) / float64(int64(uint64(k)>>33)+1)))
	}
	return int(b)
}

// effectiveTLDPlusOne returns the registrable domain (eTLD+1) for a host, or
// "" if the host is an IP or has no dot. This is a simplified, dependency-
// free version of the publicsuffix lookup mihomo uses; it treats the last
// two labels as the eTLD+1, which is correct for the common case and
// conservative (more stickiness, not less) for exotic TLDs.
func effectiveTLDPlusOne(host string) string {
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ""
	}
	// strip trailing dot
	if host[len(host)-1] == '.' {
		host = host[:len(host)-1]
	}
	// find last two dots
	lastDot := -1
	prevDot := -1
	for i := 0; i < len(host); i++ {
		if host[i] == '.' {
			prevDot = lastDot
			lastDot = i
		}
	}
	if lastDot == -1 {
		return host // single-label
	}
	if prevDot == -1 {
		return host // two labels
	}
	return host[prevDot+1:]
}

// --- least latency ---

// LeastLatency picks the candidate with the lowest smoothed recent latency,
// with a tolerance hysteresis: the current pick is only replaced if a
// candidate is more than `tolerance` faster, reducing flapping.
type LeastLatency struct {
	tolerance time.Duration
	mu        sync.Mutex
	current   string // current pick's outbound ID
}

func newLeastLatency(params map[string]any) (*LeastLatency, error) {
	l := &LeastLatency{tolerance: 50 * time.Millisecond}
	if v, ok := params["tolerance"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			l.tolerance = d
		}
	}
	if v, ok := params["tolerance_ms"].(float64); ok {
		l.tolerance = time.Duration(int64(v)) * time.Millisecond
	}
	return l, nil
}

func (l *LeastLatency) Name() string { return "leastlatency" }

func (l *LeastLatency) Pick(pool []core.Outbound, _ core.Metadata) (core.Outbound, error) {
	if len(pool) == 0 {
		return nil, ErrEmptyPool
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var best core.Outbound
	var bestLat = time.Duration(1<<63 - 1)
	for _, o := range pool {
		lat := o.Stats().RecentLatency
		if lat == 0 {
			lat = time.Second // unmeasured: assume mediocre, not zero
		}
		if lat < bestLat {
			bestLat = lat
			best = o
		}
	}
	// hysteresis: keep current if it's within tolerance of the best.
	if l.current != "" {
		for _, o := range pool {
			if o.ID() == l.current {
				curLat := o.Stats().RecentLatency
				if curLat == 0 {
					curLat = time.Second
				}
				if curLat <= bestLat+l.tolerance {
					return o, nil
				}
				break
			}
		}
	}
	if best == nil {
		best = pool[0]
	}
	l.current = best.ID()
	return best, nil
}
