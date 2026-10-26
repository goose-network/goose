package config

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func specs(ids ...string) []*OutboundSpec {
	out := make([]*OutboundSpec, len(ids))
	for i, id := range ids {
		out[i] = &OutboundSpec{ID: id, Protocol: "socks5", Config: map[string]any{}}
	}
	return out
}

func poolIDs(p *Pool) []string {
	got := append([]string(nil), p.OutboundIDs...)
	sort.Strings(got)
	return got
}

// TestProviderSpecLegacyPoolIDJSON verifies the legacy singular `pool_id`
// field still parses (old configs, old API clients) and maps onto the
// canonical pool_ids list, and that the plural form round-trips.
func TestProviderSpecLegacyPoolIDJSON(t *testing.T) {
	var legacy ProviderSpec
	if err := json.Unmarshal([]byte(`{"id":"p","provider":"psiphon","pool_id":"mypool"}`), &legacy); err != nil {
		t.Fatalf("legacy pool_id unmarshal: %v", err)
	}
	if !reflect.DeepEqual(legacy.PoolIDs, []string{"mypool"}) {
		t.Fatalf("legacy pool_id should become PoolIDs [mypool], got %v", legacy.PoolIDs)
	}
	if !reflect.DeepEqual(legacy.EffectivePoolIDs(), []string{"mypool"}) {
		t.Fatalf("EffectivePoolIDs = %v, want [mypool]", legacy.EffectivePoolIDs())
	}

	var plural ProviderSpec
	if err := json.Unmarshal([]byte(`{"id":"p","provider":"psiphon","pool_ids":["a","b"]}`), &plural); err != nil {
		t.Fatalf("pool_ids unmarshal: %v", err)
	}
	if !reflect.DeepEqual(plural.PoolIDs, []string{"a", "b"}) {
		t.Fatalf("PoolIDs = %v, want [a b]", plural.PoolIDs)
	}

	// Marshal always emits pool_ids.
	b, err := json.Marshal(plural)
	if err != nil {
		t.Fatal(err)
	}
	var echo map[string]any
	_ = json.Unmarshal(b, &echo)
	if _, ok := echo["pool_ids"]; !ok {
		t.Fatalf("marshal should emit pool_ids, got %s", b)
	}
	if _, ok := echo["pool_id"]; ok {
		t.Fatalf("marshal should not emit legacy pool_id, got %s", b)
	}

	// Both forms may appear together; the singular is prepended.
	var both ProviderSpec
	if err := json.Unmarshal([]byte(`{"id":"p","pool_id":"a","pool_ids":["b"]}`), &both); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(both.PoolIDs, []string{"a", "b"}) {
		t.Fatalf("mixed forms PoolIDs = %v, want [a b]", both.PoolIDs)
	}
}

// TestProviderSpecEffectivePoolIDs covers the defaulting/dedup rules.
func TestProviderSpecEffectivePoolIDs(t *testing.T) {
	cases := []struct {
		in   ProviderSpec
		want []string
	}{
		{ProviderSpec{}, []string{DefaultPoolID}},
		{ProviderSpec{PoolIDs: []string{""}}, []string{DefaultPoolID}},
		{ProviderSpec{PoolIDs: []string{"a", ""}}, []string{"a", DefaultPoolID}},
		{ProviderSpec{PoolIDs: []string{"a", "a", "b"}}, []string{"a", "b"}},
		{ProviderSpec{PoolIDs: nil}, []string{DefaultPoolID}},
	}
	for _, c := range cases {
		got := c.in.EffectivePoolIDs()
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("EffectivePoolIDs(%v) = %v, want %v", c.in.PoolIDs, got, c.want)
		}
	}
}

// TestSetProviderOutboundsMultiPool is the core multi-pool behavior: one
// provider's whole outbound set lands in EVERY pool it lists, and editing
// the pool list moves the outbounds between pools.
func TestSetProviderOutboundsMultiPool(t *testing.T) {
	s := NewStore()
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolIDs: []string{"fast", "fallback"}})

	s.SetProviderOutbounds("p1", specs("p1-a", "p1-b"))
	for _, poolID := range []string{"fast", "fallback"} {
		pool, ok := s.Pool(poolID)
		if !ok {
			t.Fatalf("pool %q missing", poolID)
		}
		if want := []string{"p1-a", "p1-b"}; !reflect.DeepEqual(poolIDs(pool), want) {
			t.Fatalf("pool %q = %v, want %v", poolID, poolIDs(pool), want)
		}
	}

	// Refresh replaces the set everywhere at once.
	s.SetProviderOutbounds("p1", specs("p1-a", "p1-c"))
	for _, poolID := range []string{"fast", "fallback"} {
		pool, _ := s.Pool(poolID)
		if want := []string{"p1-a", "p1-c"}; !reflect.DeepEqual(poolIDs(pool), want) {
			t.Fatalf("pool %q after refresh = %v, want %v", poolID, poolIDs(pool), want)
		}
	}

	// Edit pool_ids down to just "fast": fallback must go away (nothing
	// else feeds it) and fast must keep the full set.
	s.SetProvider(&ProviderSpec{ID: "p1", Provider: "x", PoolIDs: []string{"fast"}})
	s.SetProviderOutbounds("p1", specs("p1-a", "p1-c"))
	if _, ok := s.Pool("fallback"); ok {
		t.Fatal("fallback pool should be deleted once nothing feeds it")
	}
	pool, _ := s.Pool("fast")
	if want := []string{"p1-a", "p1-c"}; !reflect.DeepEqual(poolIDs(pool), want) {
		t.Fatalf("fast = %v, want %v", poolIDs(pool), want)
	}
}

// TestMultiPoolSharesWithOtherProvider: a multi-pool provider sharing one of
// its pools with a single-pool provider must not clobber the other's
// outbounds, and deleting either must keep the shared pool for the survivor.
func TestMultiPoolSharesWithOtherProvider(t *testing.T) {
	s := NewStore()
	s.SetProvider(&ProviderSpec{ID: "multi", Provider: "x", PoolIDs: []string{"fast", "fallback"}})
	s.SetProvider(&ProviderSpec{ID: "solo", Provider: "x", PoolIDs: []string{"fast"}})

	s.SetProviderOutbounds("multi", specs("m-a"))
	s.SetProviderOutbounds("solo", specs("s-a"))

	pool, _ := s.Pool("fast")
	if want := []string{"m-a", "s-a"}; !reflect.DeepEqual(poolIDs(pool), want) {
		t.Fatalf("shared fast = %v, want %v", poolIDs(pool), want)
	}
	pool, _ = s.Pool("fallback")
	if want := []string{"m-a"}; !reflect.DeepEqual(poolIDs(pool), want) {
		t.Fatalf("fallback = %v, want %v", poolIDs(pool), want)
	}

	// Deleting multi keeps fast (solo still feeds it) but drops fallback.
	if !s.DeleteProvider("multi") {
		t.Fatal("delete multi")
	}
	if _, ok := s.Pool("fallback"); ok {
		t.Fatal("fallback should be deleted with its only feeder")
	}
	pool, ok := s.Pool("fast")
	if !ok {
		t.Fatal("fast must survive: solo still feeds it")
	}
	if want := []string{"s-a"}; !reflect.DeepEqual(poolIDs(pool), want) {
		t.Fatalf("fast after multi deleted = %v, want %v", poolIDs(pool), want)
	}
	if _, ok := s.Outbound("m-a"); ok {
		t.Fatal("multi's outbounds should be dropped from the store")
	}
}
