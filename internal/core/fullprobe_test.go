package core

import (
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