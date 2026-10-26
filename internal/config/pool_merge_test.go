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
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolIDs: []string{"default"}})
	s.SetProvider(&ProviderSpec{ID: "p2", Provider: "x", PoolIDs: []string{"default"}})

	specs := func(ids ...string) []*OutboundSpec {
		out := make([]*OutboundSpec, len(ids))
		for i, id := range ids {
			out[i] = &OutboundSpec{ID: id, Protocol: "socks5", Config: map[string]any{}}
		}
		return out
	}

	s.SetProviderOutbounds("p1", specs("p1-a", "p1-b"))
	pool, ok := s.Pool("default")
	if !ok || len(pool.OutboundIDs) != 2 {
		t.Fatalf("p1 poll pool = %+v ok=%v", pool, ok)
	}

	// p2's poll must not evict p1's outbounds from the shared pool.
	s.SetProviderOutbounds("p2", specs("p2-a"))
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
	s.SetProviderOutbounds("p1", specs("p1-a", "p1-c"))
	pool, _ = s.Pool("default")
	got = append([]string(nil), pool.OutboundIDs...)
	sort.Strings(got)
	want = []string{"p1-a", "p1-c", "p2-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refreshed shared pool = %v, want %v", got, want)
	}

	// p2 moves to its own pool: the default pool must drop its outbounds.
	s.SetProvider(&ProviderSpec{ID: "p2", Provider: "x", PoolIDs: []string{"other"}})
	s.SetProviderOutbounds("p1", specs("p1-a", "p1-c"))
	pool, _ = s.Pool("default")
	got = append([]string(nil), pool.OutboundIDs...)
	sort.Strings(got)
	want = []string{"p1-a", "p1-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool after p2 moved away = %v, want %v", got, want)
	}
	// p2's own pool fills when its poll lands there.
	s.SetProviderOutbounds("p2", specs("p2-a"))
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

// TestSetProviderOutboundsKeepsOperatorPool verifies an operator-built pool
// that references a provider-owned outbound survives the provider's polls:
// pool membership by reference does not make a pool provider-managed (the
// regression behind the vanishing "general" pool — a user pool pinning the
// provider's general outbound was deleted on the provider's first poll,
// degrading the inbound policy silently to the all-outbounds fallback).
func TestSetProviderOutboundsKeepsOperatorPool(t *testing.T) {
	s := NewStore()
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolIDs: []string{"default"}})
	s.SetProviderOutbounds("p1", []*OutboundSpec{
		{ID: "p1-a", Protocol: "socks5", Config: map[string]any{}},
		{ID: "p1", Protocol: "socks5", Config: map[string]any{}},
	})

	// The operator hand-builds a pool pinning one provider-owned outbound.
	s.SetPool(&Pool{ID: "general", OutboundIDs: []string{"p1"}})

	// The provider's next poll (same set, or a changed set) must not touch
	// the operator pool.
	s.SetProviderOutbounds("p1", []*OutboundSpec{
		{ID: "p1-a", Protocol: "socks5", Config: map[string]any{}},
		{ID: "p1", Protocol: "socks5", Config: map[string]any{}},
	})
	pool, ok := s.Pool("general")
	if !ok {
		t.Fatal("operator pool was deleted by a provider poll")
	}
	if !reflect.DeepEqual(pool.OutboundIDs, []string{"p1"}) {
		t.Fatalf("operator pool membership rewritten: %v", pool.OutboundIDs)
	}

	// The provider moving pools must not delete the operator pool either.
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolIDs: []string{"moved"}})
	s.SetProviderOutbounds("p1", []*OutboundSpec{
		{ID: "p1", Protocol: "socks5", Config: map[string]any{}},
	})
	if _, ok := s.Pool("general"); !ok {
		t.Fatal("operator pool deleted when the provider moved to another pool")
	}

	// A provider-managed pool is still cleaned up when unfed: "default" and
	// "moved" were built by the provider, so they must be gone now.
	if _, ok := s.Pool("default"); ok {
		t.Fatal("unfed provider pool survived")
	}
	if pool, ok := s.Pool("moved"); !ok || len(pool.OutboundIDs) != 1 {
		t.Fatalf("moved pool = %+v ok=%v", pool, ok)
	}

	// Deleting the provider must keep the operator pool (it still
	// references p1, but the store tolerates dangling refs until the next
	// router rebuild drops the missing outbound from dispatch).
	s.DeleteProvider("p1")
	if _, ok := s.Pool("general"); !ok {
		t.Fatal("operator pool deleted with the provider")
	}
}
