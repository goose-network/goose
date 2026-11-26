package logfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readActive(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestManagerReconfigureLive(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	m := NewManager()
	defer m.Close()

	m.Reconfigure(a, 1000, 1000)
	if _, err := m.Write([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, a); got != "first\n" {
		t.Fatalf("a.log = %q", got)
	}

	// Identical parameters are a no-op: the same file keeps appending.
	m.Reconfigure(a, 1000, 1000)
	if _, err := m.Write([]byte("second\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, a); got != "first\nsecond\n" {
		t.Fatalf("a.log after no-op reconfigure = %q", got)
	}

	// A new path swaps writers and purges the old location: leaving the old
	// logs behind would orphan them outside the new writer's budget.
	m.Reconfigure(b, 1000, 1000)
	if _, err := m.Write([]byte("third\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, b); got != "third\n" {
		t.Fatalf("b.log = %q", got)
	}
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("a.log should have been purged on path change: %v", err)
	}

	// Empty path disables file logging; writes still succeed (stderr).
	m.Reconfigure("", 0, 0)
	if _, err := m.Write([]byte("fourth\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, b); got != "third\n" {
		t.Fatalf("b.log grew after disable: %q", got)
	}
}

func TestManagerFailedOpenKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(good, 1000, 1000)

	// A path whose parent is a regular file cannot be opened.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m.Reconfigure(filepath.Join(blocker, "bad.log"), 1000, 1000)
	if _, err := m.Write([]byte("still here\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, good); !strings.Contains(got, "still here\n") {
		t.Fatalf("previous writer lost after failed reconfigure: %q", got)
	}
}

func TestManagerBudgetEnforcedEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(path, 100, 350)
	line := strings.Repeat("z", 59) + "\n"
	for i := 0; i < 40; i++ {
		if _, err := m.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			total += info.Size()
		}
		if total > 350 {
			t.Fatalf("iteration %d: total %d exceeds 350", i, total)
		}
	}
}

// TestManagerDisableThenMovePurgesRetired: disabling file logging keeps the
// logs for inspection, but re-enabling at a DIFFERENT path must purge the
// retired location — otherwise its bytes orphan outside the new budget.
func TestManagerDisableThenMovePurgesRetired(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(a, 1000, 1000)
	if _, err := m.Write([]byte("history\n")); err != nil {
		t.Fatal(err)
	}
	m.Reconfigure("", 0, 0)
	if got := readActive(t, a); got != "history\n" {
		t.Fatalf("disable should retain logs for inspection, got %q", got)
	}
	m.Reconfigure(b, 1000, 1000)
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("retired location should be purged on re-enable elsewhere: %v", err)
	}
	if _, err := m.Write([]byte("new\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, b); got != "new\n" {
		t.Fatalf("b.log = %q", got)
	}
}

// TestManagerClosedReconfigureNoop: a config-watch iteration racing shutdown
// must not resurrect a file writer nothing will ever close.
func TestManagerClosedReconfigureNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	m := NewManager()
	m.Reconfigure(path, 1000, 1000)
	m.Close()
	m.Reconfigure(path, 1000, 1000)
	if _, err := m.Write([]byte("stderr only\n")); err != nil {
		t.Fatalf("Write after Close should still work (stderr): %v", err)
	}
	if got := readActive(t, path); got != "" {
		t.Fatalf("closed manager wrote to file after reconfigure: %q", got)
	}
}

// TestManagerRepeatDisableKeepsRetirement: the watch loop passes resolved
// POSITIVE budgets even when logging is disabled, so every config version
// bump re-enters Reconfigure(""). A repeat disable must not erase the
// retirement record — the retained logs must still be purged on a later move.
func TestManagerRepeatDisableKeepsRetirement(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(a, 1000, 2000)
	if _, err := m.Write([]byte("history\n")); err != nil {
		t.Fatal(err)
	}
	m.Reconfigure("", 32_000_000, 1_000_000_000)
	// Two more watch iterations while disabled, with the same positive
	// budgets Resolve() produces for "-".
	m.Reconfigure("", 32_000_000, 1_000_000_000)
	m.Reconfigure("", 32_000_000, 1_000_000_000)
	m.Reconfigure(b, 32_000_000, 1_000_000_000)
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("retained logs survived repeat disables + move: %v", err)
	}
}

// TestManagerAliasedPathNotPurged: spelling the SAME location differently
// must be treated as a same-path reconfigure, not a path change that purges
// the live writer's files.
func TestManagerAliasedPathNotPurged(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(a, 1000, 1000)
	if _, err := m.Write([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	m.Reconfigure(filepath.Join(dir, ".", "a.log"), 1000, 1000)
	if _, err := m.Write([]byte("second\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, a); got != "first\nsecond\n" {
		t.Fatalf("aliased reconfigure destroyed history: %q", got)
	}
}

// TestManagerFailedPurgeRetried: a purge that fails (read-only directory)
// must keep the location recorded and retry on a later Reconfigure once the
// filesystem heals — not orphan a full budget forever.
func TestManagerFailedPurgeRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not constrain root")
	}
	dirA := filepath.Join(t.TempDir(), "a")
	dirB := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(dirA, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dirA, 0o755) })
	a := filepath.Join(dirA, "a.log")
	b := filepath.Join(dirB, "b.log")
	m := NewManager()
	defer m.Close()
	m.Reconfigure(a, 1000, 2000)
	if _, err := m.Write([]byte("history\n")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dirA, 0o555); err != nil {
		t.Fatal(err)
	}
	m.Reconfigure(b, 1000, 2000) // swap succeeds; purge of A fails (EACCES)
	if _, err := os.Stat(a); err != nil {
		t.Fatalf("a.log should still exist while its dir is unwritable: %v", err)
	}
	if err := os.Chmod(dirA, 0o755); err != nil {
		t.Fatal(err)
	}
	// A later reconfigure (any version bump with the same params is a no-op,
	// so change the budget the way a real edit would) retries the purge.
	m.Reconfigure(b, 1000, 1900)
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("failed purge was never retried: %v", err)
	}
}

// TestManagerRetirementSurvivesRestart: disable retains the logs AND the
// ledger; a fresh Manager (new process) re-enabling elsewhere must purge the
// retained location even though it never saw the disable.
func TestManagerRetirementSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	m1 := NewManager()
	m1.Reconfigure(a, 1000, 2000)
	if _, err := m1.Write([]byte("history\n")); err != nil {
		t.Fatal(err)
	}
	m1.Reconfigure("", 32_000_000, 1_000_000_000)
	m1.Close()
	if got := readActive(t, a); got != "history\n" {
		t.Fatalf("disable should retain logs for inspection: %q", got)
	}
	// "Restart": a brand-new Manager knows nothing of A except the ledger.
	m2 := NewManager()
	defer m2.Close()
	m2.Reconfigure(b, 1000, 2000)
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("retained logs orphaned across restart: %v", err)
	}
	if _, err := m2.Write([]byte("new\n")); err != nil {
		t.Fatal(err)
	}
	if got := readActive(t, b); got != "new\n" {
		t.Fatalf("b.log = %q", got)
	}
}