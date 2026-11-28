package core

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNormalizeFullProbeSpecDefaults(t *testing.T) {
	s, err := NormalizeFullProbeSpec(FullProbeSpec{Target: "http://example.com"})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if s.MaxDepth != DefaultFullProbeDepth {
		t.Errorf("MaxDepth = %d, want default %d", s.MaxDepth, DefaultFullProbeDepth)
	}
	if s.RPS != DefaultFullProbeRPS {
		t.Errorf("RPS = %d, want default %d", s.RPS, DefaultFullProbeRPS)
	}
	if s.Timeout != DefaultFullProbeTimeout {
		t.Errorf("Timeout = %v, want default %v", s.Timeout, DefaultFullProbeTimeout)
	}
	if s.Concurrency != 2*DefaultFullProbeRPS {
		t.Errorf("Concurrency = %d, want 2×RPS", s.Concurrency)
	}
	if s.MaxAttemptsPerLevel != DefaultFullProbeAttemptsPerLevel ||
		s.MaxPathsPerLevel != DefaultFullProbePathsPerLevel ||
		s.MaxTotalAttempts != DefaultFullProbeTotalAttempts {
		t.Errorf("fan-out defaults not applied: %+v", s)
	}
	if s.CatchupSeconds != DefaultFullProbeCatchupSeconds {
		t.Errorf("CatchupSeconds = %d, want default %d", s.CatchupSeconds, DefaultFullProbeCatchupSeconds)
	}
}

func TestNormalizeFullProbeSpecCaps(t *testing.T) {
	s, err := NormalizeFullProbeSpec(FullProbeSpec{
		Target:              "https://example.com/x",
		MaxDepth:            99,
		RPS:                 999999,
		Timeout:             time.Hour,
		Concurrency:         999999,
		MaxAttemptsPerLevel: 999999,
		MaxPathsPerLevel:    999999,
		MaxTotalAttempts:    9999999,
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if s.MaxDepth != FullProbeMaxDepth {
		t.Errorf("MaxDepth = %d, want cap %d", s.MaxDepth, FullProbeMaxDepth)
	}
	if s.RPS != FullProbeMaxRPS {
		t.Errorf("RPS = %d, want cap %d", s.RPS, FullProbeMaxRPS)
	}
	if s.Timeout != FullProbeMaxTimeout {
		t.Errorf("Timeout = %v, want cap %v", s.Timeout, FullProbeMaxTimeout)
	}
	if s.Concurrency != FullProbeMaxConcurrency {
		t.Errorf("Concurrency = %d, want cap %d", s.Concurrency, FullProbeMaxConcurrency)
	}
	if s.MaxAttemptsPerLevel != FullProbeMaxAttemptsPerLevel ||
		s.MaxPathsPerLevel != FullProbeMaxPathsPerLevel ||
		s.MaxTotalAttempts != FullProbeMaxTotalAttempts {
		t.Errorf("fan-out caps not applied: %+v", s)
	}
}

func TestNormalizeFullProbeSpecCatchup(t *testing.T) {
	// Above the cap clamps down; negative disables the tail and normalizes
	// to -1 (a fixed point, so the stored spec round-trips); a positive
	// value under the cap passes through unchanged.
	for _, tc := range []struct {
		in, want int
	}{
		{FullProbeMaxCatchupSeconds + 1000, FullProbeMaxCatchupSeconds},
		{-1, -1},
		{-9999, -1},
		{120, 120},
	} {
		s, err := NormalizeFullProbeSpec(FullProbeSpec{Target: "http://x.com", CatchupSeconds: tc.in})
		if err != nil {
			t.Fatalf("normalize(%d): %v", tc.in, err)
		}
		if s.CatchupSeconds != tc.want {
			t.Errorf("CatchupSeconds(%d) = %d, want %d", tc.in, s.CatchupSeconds, tc.want)
		}
		// Normalization must be idempotent: re-normalizing a stored spec
		// (e.g. a client replaying it) must not change the tail behavior.
		again, err := NormalizeFullProbeSpec(s)
		if err != nil {
			t.Fatalf("re-normalize(%d): %v", tc.in, err)
		}
		if again.CatchupSeconds != s.CatchupSeconds {
			t.Errorf("CatchupSeconds(%d) not a fixed point: %d -> %d", tc.in, s.CatchupSeconds, again.CatchupSeconds)
		}
	}
}

func TestNormalizeFullProbeSpecConcurrencyFloor(t *testing.T) {
	s, err := NormalizeFullProbeSpec(FullProbeSpec{Target: "http://x.com", RPS: 1})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if s.Concurrency < 16 {
		t.Errorf("Concurrency = %d, want floor 16", s.Concurrency)
	}
}

func TestNormalizeFullProbeSpecBadTarget(t *testing.T) {
	for _, bad := range []string{"", "not-a-url", "ftp://x.com", "example.com"} {
		if _, err := NormalizeFullProbeSpec(FullProbeSpec{Target: bad}); err == nil {
			t.Errorf("target %q: want error", bad)
		}
	}
}

func TestFullProbeBestPaths(t *testing.T) {
	j := &FullProbeJob{Levels: []FullProbeLevel{
		{Depth: 1, SuccessPaths: []FullProbePath{
			{Hops: []string{"slow"}, Latency: 30 * time.Millisecond},
			{Hops: []string{"fast"}, Latency: 10 * time.Millisecond},
		}},
		{Depth: 2, SuccessPaths: []FullProbePath{
			{Hops: []string{"fast", "b"}, Latency: 20 * time.Millisecond},
		}},
		{Depth: 3, SuccessPaths: nil}, // no success at this depth
	}}
	best := j.BestPaths()
	if len(best) != 2 {
		t.Fatalf("BestPaths len = %d, want 2 (depth 3 has none)", len(best))
	}
	if best[0].Hops[0] != "fast" {
		t.Errorf("depth-1 best = %v, want fastest (fast)", best[0].Hops)
	}
	if len(best[1].Hops) != 2 || best[1].Hops[1] != "b" {
		t.Errorf("depth-2 best = %v, want [fast b]", best[1].Hops)
	}
}

func TestFullProbeCloneIndependent(t *testing.T) {
	j := &FullProbeJob{
		ID:    "fp-1",
		State: FullProbeRunning,
		Levels: []FullProbeLevel{
			{Depth: 1, SuccessPaths: []FullProbePath{{Hops: []string{"a"}}},
				FailedItems: []FullProbeFailedItem{{ID: "b", Count: 1}}},
		},
	}
	c := j.Clone()
	// Mutating the clone must not touch the original.
	c.Levels[0].SuccessPaths[0].Hops[0] = "mutated"
	c.Levels[0].FailedItems[0].Count = 99
	c.State = FullProbeDone
	if j.Levels[0].SuccessPaths[0].Hops[0] != "a" {
		t.Errorf("clone shares Hops backing array")
	}
	if j.Levels[0].FailedItems[0].Count != 1 {
		t.Errorf("clone shares FailedItems backing array")
	}
	if j.State != FullProbeRunning {
		t.Errorf("clone shares State")
	}
}

func TestFullProbeRowsPrunedField(t *testing.T) {
	// The additive flag must stay invisible on unpruned jobs: their JSON is
	// byte-identical to before the field existed.
	j := &FullProbeJob{ID: "fp-1", State: FullProbeDone}
	unmarked, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(unmarked), "rows_pruned") {
		t.Errorf("unpruned job JSON must omit rows_pruned: %s", unmarked)
	}
	j.RowsPruned = true
	marked, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(marked), `"rows_pruned":true`) {
		t.Errorf("pruned job JSON must carry rows_pruned: %s", marked)
	}
	if c := j.Clone(); !c.RowsPruned {
		t.Errorf("Clone must carry RowsPruned")
	}
}