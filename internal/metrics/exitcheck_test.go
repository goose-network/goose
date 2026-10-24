package metrics

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreExitChecksRoundtrip records checks and verifies persistence,
// ordering (newest first), and the outbound filter.
func TestStoreExitChecksRoundtrip(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "m.db"))
	require.NoError(t, err)
	defer s.Close()

	base := time.Now().Add(-time.Hour)
	recs := []core.ExitCheck{
		{ID: "c1", OutboundID: "a", Site: "ifconfig.me", ExitIP: "1.2.3.4", Success: true, Latency: 120 * time.Millisecond, StartedAt: base},
		{ID: "c2", OutboundID: "b", Site: "api.ipify.org", ExitIP: "5.6.7.8", Success: true, Latency: 80 * time.Millisecond, StartedAt: base.Add(time.Minute)},
		{ID: "c3", OutboundID: "a", Site: "ipinfo.io", ExitIP: "", Success: false, Error: "dial: timeout", Latency: time.Second, StartedAt: base.Add(2 * time.Minute)},
	}
	for _, c := range recs {
		require.NoError(t, s.RecordExitCheck(c))
	}

	all, err := s.RecentExitChecks(10, "")
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, "c3", all[0].ID, "newest first")
	assert.Equal(t, "c1", all[2].ID, "oldest last")
	assert.Equal(t, "ifconfig.me", all[2].Site)
	assert.Equal(t, "1.2.3.4", all[2].ExitIP)
	assert.False(t, all[0].Success)
	assert.Equal(t, "dial: timeout", all[0].Error)

	// outbound filter
	onlyA, err := s.RecentExitChecks(10, "a")
	require.NoError(t, err)
	require.Len(t, onlyA, 2)
	for _, c := range onlyA {
		assert.Equal(t, "a", c.OutboundID)
	}

	// n limit
	one, err := s.RecentExitChecks(1, "")
	require.NoError(t, err)
	assert.Len(t, one, 1)
}

// TestStoreExitChecksCollidingTimestamps verifies two checks recorded at the
// same nanosecond-ish instant do not clobber each other (the key carries both
// timestamp and ID).
func TestStoreExitChecksCollidingTimestamps(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "m.db"))
	require.NoError(t, err)
	defer s.Close()

	ts := time.Now()
	for i := 0; i < 5; i++ {
		require.NoError(t, s.RecordExitCheck(core.ExitCheck{
			ID: string(rune('a' + i)), OutboundID: "a", Site: "ifconfig.me",
			ExitIP: "1.2.3.4", Success: true, StartedAt: ts,
		}))
	}
	all, err := s.RecentExitChecks(100, "")
	require.NoError(t, err)
	assert.Len(t, all, 5, "same-timestamp checks with distinct IDs must all persist")
}
