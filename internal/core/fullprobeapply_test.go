package core

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestFullProbeApplySpecPoolFieldsJSON pins the additive wire contract of
// the pool-mode apply fields: the zero spec marshals byte-identically to the
// pre-pool form (no new keys), and mode/pool_id/selector round-trip.
func TestFullProbeApplySpecPoolFieldsJSON(t *testing.T) {
	b, err := json.Marshal(FullProbeApplySpec{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != "{}" {
		t.Fatalf("zero apply spec JSON = %s, want {}", b)
	}
	legacy, err := json.Marshal(FullProbeApplySpec{Paths: [][]string{{"a"}}, Port: 1080})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{"mode", "pool_id", "selector"} {
		if strings.Contains(string(legacy), key) {
			t.Fatalf("inbounds-mode spec JSON must omit %q: %s", key, legacy)
		}
	}

	in := FullProbeApplySpec{
		Mode: "pool", PoolID: "p",
		Selector: &FullProbeSelectorSpec{Type: "sticky", Params: map[string]any{"key": "domain"}},
	}
	b, err = json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back FullProbeApplySpec
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, back) {
		t.Fatalf("round trip mismatch:\nin  %+v\nout %+v", in, back)
	}
}

// TestFullProbeAllPathsFastest pins the pool-mode default selection: every
// successful path across levels, deduped by hop-join (first occurrence
// wins), ordered fastest first with shallower paths winning latency ties.
func TestFullProbeAllPathsFastest(t *testing.T) {
	j := &FullProbeJob{Levels: []FullProbeLevel{
		{Depth: 1, SuccessPaths: []FullProbePath{
			{Hops: []string{"a"}, Latency: 30 * time.Millisecond},
			{Hops: []string{"b"}, Latency: 10 * time.Millisecond},
		}},
		{Depth: 2, SuccessPaths: []FullProbePath{
			{Hops: []string{"a", "c"}, Latency: 10 * time.Millisecond}, // ties b: shallower first
			{Hops: []string{"a"}, Latency: 5 * time.Millisecond},       // duplicate of depth-1 a: dropped
			{Hops: []string{"a", "d"}, Latency: 50 * time.Millisecond},
		}},
		{Depth: 3, SuccessPaths: nil},
	}}
	got := j.AllPathsFastest()
	want := [][]string{{"b"}, {"a", "c"}, {"a"}, {"a", "d"}}
	if len(got) != len(want) {
		t.Fatalf("AllPathsFastest len = %d, want %d (%+v)", len(got), len(want), got)
	}
	for i, w := range want {
		if !reflect.DeepEqual(got[i].Hops, w) {
			t.Errorf("path %d = %v, want %v", i, got[i].Hops, w)
		}
	}
	if got[2].Latency != 30*time.Millisecond {
		t.Errorf("dedup must keep the first occurrence's latency, got %v", got[2].Latency)
	}

	empty := &FullProbeJob{}
	if paths := empty.AllPathsFastest(); len(paths) != 0 {
		t.Errorf("empty job paths = %+v, want none", paths)
	}
}
