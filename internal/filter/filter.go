// Package filter implements the predicates that narrow a pool of outbounds
// before the selector picks one. Filters compose per-inbound (and per-chain
// layer), giving each port+auth its own admission rules. All filters are
// safe for concurrent use.
package filter

import (
	"errors"
	"strings"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// New builds a filter from a type name + params.
func New(typ string, params map[string]any) (core.Filter, error) {
	switch typ {
	case "location":
		return newLocation(params)
	case "latency":
		return newLatency(params)
	case "protocol":
		return newProtocol(params)
	default:
		return nil, errors.New("filter: unknown type " + typ)
	}
}

// --- location ---

// Location admits outbounds whose resolved country (or province/city) is in
// an allowlist, and/or not in a denylist.
type Location struct {
	countries map[string]struct{}
	provinces map[string]struct{}
	exclude   map[string]struct{}
}

func newLocation(params map[string]any) (*Location, error) {
	f := &Location{
		countries: set(params, "countries"),
		provinces: set(params, "provinces"),
		exclude:   set(params, "exclude"),
	}
	return f, nil
}

func (l *Location) Name() string { return "location" }

func (l *Location) Allow(o core.Outbound, _ core.Metadata) bool {
	loc := o.Location()
	if loc == nil {
		// no location known: admit only if no positive allowlist is set.
		return len(l.countries) == 0 && len(l.provinces) == 0
	}
	if _, bad := l.exclude[strings.ToLower(loc.Country)]; bad {
		return false
	}
	if len(l.countries) == 0 && len(l.provinces) == 0 {
		return true
	}
	if _, ok := l.countries[strings.ToLower(loc.Country)]; ok {
		return true
	}
	if _, ok := l.provinces[strings.ToLower(loc.Province)]; ok {
		return true
	}
	return false
}

// --- latency ---

// Latency admits outbounds whose smoothed recent latency is at most Max and
// whose success rate is at least MinSuccess. Unmeasured outbounds are
// admitted (so a fresh pool isn't locked out).
type Latency struct {
	Max        time.Duration
	MinSuccess float64
}

func newLatency(params map[string]any) (*Latency, error) {
	f := &Latency{Max: 0, MinSuccess: 0}
	if v, ok := params["max_ms"].(float64); ok {
		f.Max = time.Duration(int64(v)) * time.Millisecond
	}
	if v, ok := params["max"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			f.Max = d
		}
	}
	if v, ok := params["min_success"].(float64); ok {
		f.MinSuccess = v
	}
	return f, nil
}

func (l *Latency) Name() string { return "latency" }

func (l *Latency) Allow(o core.Outbound, _ core.Metadata) bool {
	st := o.Stats()
	if st.RecentLatency == 0 {
		return true // unmeasured
	}
	if l.Max > 0 && st.RecentLatency > l.Max {
		return false
	}
	if l.MinSuccess > 0 && st.SuccessRate < l.MinSuccess {
		return false
	}
	return true
}

// --- protocol (whitelist / blacklist) ---

// Protocol admits outbounds whose protocol is in a whitelist (if set) and
// not in a blacklist.
type Protocol struct {
	allow map[string]struct{}
	deny  map[string]struct{}
}

func newProtocol(params map[string]any) (*Protocol, error) {
	return &Protocol{
		allow: set(params, "allow"),
		deny:  set(params, "deny"),
	}, nil
}

func (p *Protocol) Name() string { return "protocol" }

func (p *Protocol) Allow(o core.Outbound, _ core.Metadata) bool {
	proto := strings.ToLower(o.Protocol())
	if _, bad := p.deny[proto]; bad {
		return false
	}
	if len(p.allow) > 0 {
		if _, ok := p.allow[proto]; !ok {
			return false
		}
	}
	return true
}

// set builds a lowercase string set from a []any config value.
func set(params map[string]any, key string) map[string]struct{} {
	out := map[string]struct{}{}
	v, ok := params[key]
	if !ok {
		return out
	}
	list, ok := v.([]any)
	if !ok {
		return out
	}
	for _, item := range list {
		if s, ok := item.(string); ok {
			out[strings.ToLower(s)] = struct{}{}
		}
	}
	return out
}
