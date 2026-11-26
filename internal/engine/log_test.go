package engine

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
)

// logRecord writes marker via the standard logger repeatedly until path
// contains it (the watch goroutine applies config changes asynchronously,
// and the subscribe channel coalesces bumps, so a single record can race a
// pending reconfigure) or fails the test.
func logRecord(t *testing.T, path, marker string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		log.Printf("engine-log-test: %s", marker)
		if b, err := os.ReadFile(path); err == nil && strings.Contains(string(b), marker) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := os.ReadFile(path)
	t.Fatalf("%s never contained %q; got %q", path, marker, b)
}

func TestEngineLogFileCaptureAndHotReconfigure(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	cfg := config.NewStore()
	eng := config.Engine{Stack: "system", DB: filepath.Join(dir, "engine.db")}
	eng.Log = config.LogConfig{File: a, MaxSizeMB: 1, MaxTotalMB: 1}
	cfg.SetEngine(eng)
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { e.Close() })

	logRecord(t, a, "record one")

	// Hot swap to a new path through a config edit; the watch goroutine
	// reconfigures the log manager on the version bump.
	eng.Log.File = b
	cfg.SetEngine(eng)
	logRecord(t, b, "record two")
	// The path change purges the old location: a.log must be gone entirely,
	// and must not reappear no matter how many further records are emitted.
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("old log file should have been purged on path change: %v", err)
	}
	logRecord(t, b, "record two-b")
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("old log file reappeared after swap: %v", err)
	}

	// "-" disables file logging; the standard logger keeps working. Poll a
	// probe record until b.log stops growing: once a record lands only on
	// stderr the swap has been applied by the watch goroutine.
	eng.Log.File = "-"
	cfg.SetEngine(eng)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		before, _ := os.ReadFile(b)
		log.Printf("engine-log-test: disable probe")
		time.Sleep(20 * time.Millisecond)
		if after, _ := os.ReadFile(b); len(after) == len(before) {
			break
		}
	}
	log.Printf("engine-log-test: record three")
	time.Sleep(50 * time.Millisecond)
	if data, _ := os.ReadFile(b); strings.Contains(string(data), "record three") {
		t.Fatal("disabled file logging still wrote to b.log")
	}
}

func TestEngineCloseRestoresLogOutput(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewStore()
	eng := config.Engine{Stack: "system", DB: filepath.Join(dir, "engine.db")}
	eng.Log = config.LogConfig{File: filepath.Join(dir, "x.log")}
	cfg.SetEngine(eng)
	prev := log.Writer()
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	e.Close()
	if log.Writer() != prev {
		t.Fatal("Close did not restore the standard logger's previous output")
	}
}