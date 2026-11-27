package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordFullProbeJobClosedSentinel asserts the terminal write on a closed
// store returns the ErrClosed sentinel: the router's terminal-persist retry
// matches it with errors.Is to stop retrying a store that will never accept
// the record again, so a plain errors.New would strand the retry loop.
func TestRecordFullProbeJobClosedSentinel(t *testing.T) {
	s, err := Open(t.TempDir() + "/closed.db")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err = s.RecordFullProbeJob(&core.FullProbeJob{
		ID:         fpJobID(1),
		State:      core.FullProbeDone,
		FinishedAt: time.Now(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrClosed), "closed-store rejection must be ErrClosed, got: %v", err)
	assert.Equal(t, "metrics: store closed", err.Error(), "message must stay unchanged for existing consumers")
}
