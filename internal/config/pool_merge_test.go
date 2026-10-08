package config

import (
	"reflect"
	"sort"
	"testing"
)

// TestSetProviderOutboundsSharedPool verifies two providers targeting the
// same pool accumulate instead of clobbering: each poll keeps the OTHER
// provider's outbounds in the pool alongside its own.
func TestSetProviderOutboundsSharedPool(t *testing.T) {
	s := NewStore()
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolID: "default"})
	s.SetProvider(&ProviderSpec{ID: "p2", Provider: "x", PoolID: "default"})

	specs := func(ids ...string) []*OutboundSpec {
		out := make([]*OutboundSpec, len(ids))
		for i, id := range ids {
			out[i] = &OutboundSpec{ID: id, Protocol: "socks5", Config: map[string]any{}}
		}
		return out
	}

	s.SetProviderOutbounds("p1", "default", specs("p1-a", "p1-b"))
	pool, ok := s.Pool("default")
	if !ok || len(pool.OutboundIDs) != 2 {
		t.Fatalf("p1 poll pool = %+v ok=%v", pool, ok)
	}

	// p2's poll must not evict p1's outbounds from the shared pool.
	s.SetProviderOutbounds("p2", "default", specs("p2-a"))
	pool, ok = s.Pool("default")
	if !ok {
		t.Fatal("pool missing")
	}
	got := append([]string(nil), pool.OutboundIDs...)
	sort.Strings(got)
	want := []string{"p1-a", "p1-b", "p2-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shared pool = %v, want %v", got, want)
	}

	// p1 refreshes with a changed set: p1-b gone, p1-c in; p2-a stays.
	s.SetProviderOutbounds("p1", "default", specs("p1-a", "p1-c"))
	pool, _ = s.Pool("default")
	got = append([]string(nil), pool.OutboundIDs...)
	sort.Strings(got)
	want = []string{"p1-a", "p1-c", "p2-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refreshed shared pool = %v, want %v", got, want)
	}

	// p2 moves to its own pool: the default pool must drop its outbounds.
	s.SetProvider(&ProviderSpec{ID: "p2", Provider: "x", PoolID: "other"})
	s.SetProviderOutbounds("p1", "default", specs("p1-a", "p1-c"))
	pool, _ = s.Pool("default")
	got = append([]string(nil), pool.OutboundIDs...)
	sort.Strings(got)
	want = []string{"p1-a", "p1-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool after p2 moved away = %v, want %v", got, want)
	}
	// p2's own pool fills when its poll lands there.
	s.SetProviderOutbounds("p2", "other", specs("p2-a"))
	pool, _ = s.Pool("other")
	if len(pool.OutboundIDs) != 1 || pool.OutboundIDs[0] != "p2-a" {
		t.Fatalf("other pool = %v", pool.OutboundIDs)
	}

	// A stale outbound owned by p1 but removed from the store (e.g. an ID
	// taken over by another provider) must not be resurrected into the pool.
	if _, ok := s.Outbound("p1-b"); ok {
		t.Fatal("p1-b should have been removed from the store")
	}
}
