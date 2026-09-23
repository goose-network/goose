package metrics

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreRecordAndStats(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "m.db"))
	require.NoError(t, err)
	defer s.Close()

	now := time.Now()
	for i := 0; i < 5; i++ {
		require.NoError(t, s.Record(core.RequestMetric{
			ID: "r", InboundID: "in", Chain: []string{"ob1"},
			Target: "example.com:443", Success: true,
			Latency: 50 * time.Millisecond, StartedAt: now, FinishedAt: now.Add(50 * time.Millisecond),
		}))
	}
	st := s.Stats("ob1")
	assert.InDelta(t, 1.0, st.SuccessRate, 0.001, "all 5 succeeded")
	assert.Greater(t, st.RecentLatency, time.Duration(0))
}

func TestStoreRecordsFailure(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "m.db"))
	defer s.Close()
	require.NoError(t, s.Record(core.RequestMetric{ID: "r", Chain: []string{"ob"}, Success: false, Error: "dial: timeout", StartedAt: time.Now(), FinishedAt: time.Now()}))
	st := s.Stats("ob")
	assert.Equal(t, 0.0, st.SuccessRate)
}

func TestStoreRecent(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "m.db"))
	defer s.Close()
	for i := 0; i < 10; i++ {
		require.NoError(t, s.Record(core.RequestMetric{ID: "r", Chain: []string{"ob"}, Success: true, StartedAt: time.Now(), FinishedAt: time.Now()}))
	}
	// give the async writer a moment
	time.Sleep(700 * time.Millisecond)
	rec, err := s.Recent(5)
	require.NoError(t, err)
	assert.Len(t, rec, 5)
}

func TestStoreNoChainSkipsStats(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "m.db"))
	defer s.Close()
	// a metric with no chain should not crash and should still persist.
	require.NoError(t, s.Record(core.RequestMetric{ID: "r", Chain: nil, Success: true, StartedAt: time.Now(), FinishedAt: time.Now()}))
}
