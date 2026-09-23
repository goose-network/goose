package metrics

import (
	"sync"
	"testing"

	"github.com/goose-network/goose/internal/core"
)

// TestRecordDuringCloseNoRace hammers Record concurrently with Close to prove
// the sendMu + closed-flag guard prevents "send on closed channel" panics and
// is race-free under the detector. This is the regression test for the
// Record-vs-Close race: a naive `close(s.in)` with a bare `s.in <- m` send
// panics or races; the implementation double-checks the atomic closed flag
// under the read lock so a Record that observes !closed is guaranteed the
// channel is still open for its send.
func TestRecordDuringCloseNoRace(t *testing.T) {
	s, err := Open(t.TempDir() + "/race.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const writers = 16
	const recordsPerWriter = 200
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < recordsPerWriter; j++ {
				// Record must never panic, regardless of whether Close has
				// run concurrently. A closed store returns nil silently.
				_ = s.Record(core.RequestMetric{
					ID:        "m",
					InboundID: "in",
					Network:   core.NetworkTCP,
					Target:    "example.com:80",
					Chain:     []string{"ob1"},
					Success:   true,
				})
			}
		}()
	}

	// Close while writers are still firing. The race detector flags any
	// concurrent read/write of the channel or closed flag without the lock.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
}

// TestRecordAfterCloseReturnsCleanly asserts that once Close has returned,
// further Record calls are safe no-ops (no panic, no error that callers must
// special-case beyond ignoring).
func TestRecordAfterCloseReturnsCleanly(t *testing.T) {
	s, err := Open(t.TempDir() + "/after.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Multiple records after close must all be safe.
	for i := 0; i < 10; i++ {
		if err := s.Record(core.RequestMetric{ID: "x", Chain: []string{"ob"}}); err != nil {
			t.Fatalf("Record after Close returned error: %v", err)
		}
	}
}
