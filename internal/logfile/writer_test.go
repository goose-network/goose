package logfile

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// footprint sums the active file and every recognized rotation on disk.
func footprint(t *testing.T, dir, active string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name != active && !strings.HasPrefix(name, strings.TrimSuffix(active, filepath.Ext(active))+"-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func TestWriterRotationAndBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 100, 350)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	chunk := bytes.Repeat([]byte("x"), 60)
	for i := 0; i < 50; i++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if got := footprint(t, dir, "goose.log"); got > 350 {
			t.Fatalf("write %d: footprint %d exceeds budget 350", i, got)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 100 {
			t.Fatalf("write %d: active file %d exceeds max size 100", i, info.Size())
		}
	}
	// After 3000 bytes written under a 350-byte budget the directory must
	// actually hold data (pruning works by deleting, not by dropping writes).
	if got := footprint(t, dir, "goose.log"); got == 0 || got > 350 {
		t.Fatalf("final footprint %d out of range (0, 350]", got)
	}
}

func TestWriterClampsMaxSizeToBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 5000, 300) // maxSize > maxTotal: clamp
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	chunk := bytes.Repeat([]byte("y"), 120)
	for i := 0; i < 20; i++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if got := footprint(t, dir, "goose.log"); got > 300 {
			t.Fatalf("write %d: footprint %d exceeds budget 300", i, got)
		}
	}
}

func TestWriterOversizedRecordRejected(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "goose.log"), 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("z"), 11)); err == nil {
		t.Fatal("write larger than max size should fail")
	}
	// The rejected record must not have landed in the file.
	if got := footprint(t, dir, "goose.log"); got != 0 {
		t.Fatalf("footprint %d after rejected write, want 0", got)
	}
}

func TestWriterStartupPrunesPreexistingState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	// An oversized active file and old rotations from a previous run.
	big := bytes.Repeat([]byte("a"), 400)
	if err := os.WriteFile(path, big, 0o644); err != nil {
		t.Fatal(err)
	}
	for i, ts := range []string{
		"20200101T000000.000000000Z",
		"20210101T000000.000000000Z",
		"20220101T000000.000000000Z",
	} {
		if err := os.WriteFile(filepath.Join(dir, "goose-"+ts+".log"), bytes.Repeat([]byte("b"), 200+i), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// An unrelated file sharing the directory must survive untouched.
	other := filepath.Join(dir, "goose-notes.log")
	if err := os.WriteFile(other, []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(path, 100, 350)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if got := footprint(t, dir, "goose.log"); got > 350 {
		t.Fatalf("startup footprint %d exceeds budget 350", got)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("unrelated file was pruned: %v", err)
	}
	// The oldest rotation must have been deleted first.
	if _, err := os.Stat(filepath.Join(dir, "goose-20200101T000000.000000000Z.log")); !os.IsNotExist(err) {
		t.Fatal("oldest rotation should have been pruned")
	}
	// Writing still works after the startup prune.
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
}

func TestWriterRotationNameCollision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 4, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	// Hammer rotations and require every rotated file to be distinct and
	// parseable (a clock collision must bump the name, never overwrite).
	for i := 0; i < 25; i++ {
		if _, err := w.Write([]byte("abcd")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Name() == "goose.log" {
			continue
		}
		if seen[e.Name()] {
			t.Fatalf("duplicate rotation %s", e.Name())
		}
		seen[e.Name()] = true
		mid := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "goose-"), ".log")
		if _, err := time.Parse(tsLayout, mid); err != nil {
			t.Fatalf("rotation %s has unparseable timestamp: %v", e.Name(), err)
		}
	}
	if len(seen) < 20 {
		t.Fatalf("expected ~24 rotations, got %d", len(seen))
	}
}

func TestWriterInvalidBudget(t *testing.T) {
	if _, err := NewWriter(filepath.Join(t.TempDir(), "x.log"), 10, 0); err == nil {
		t.Fatal("zero maxTotal should fail")
	}
}

func TestWriterLongBaseNameRejected(t *testing.T) {
	// A base name whose rotation form would exceed NAME_MAX must be rejected
	// up front: Stat on an over-long name errors forever and must never be
	// mistaken for a name collision.
	long := strings.Repeat("n", 240) + ".log"
	if _, err := NewWriter(filepath.Join(t.TempDir(), long), 10, 100); err == nil {
		t.Fatal("over-long base name should be rejected")
	}
}

// TestWriterRotateStatErrorReturns guards the collision loop: a Stat error
// other than NotExist (here EACCES from an unreadable directory) must abort
// the rotation immediately, not spin forever bumping the timestamp.
func TestWriterRotateStatErrorReturns(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not constrain root")
	}
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 50, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("x"), 40)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	// Rotation triggers (40+20 > 50); Stat fails with EACCES. The record is
	// kept through the still-open handle; the call must return promptly.
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatalf("write during stat failure should keep the record: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Once the cause clears, the next rotation succeeds (self-healing).
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatal(err)
	}
	rots, err := scanRotations(dir, "goose.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(rots) == 0 {
		t.Fatal("writer never recovered after the transient stat failure")
	}
}

// TestWriterRotateFailureRecovers exercises the rename-failure path: the
// active file is reopened, records keep landing, and rotation resumes when
// the filesystem recovers.
func TestWriterRotateFailureRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not constrain root")
	}
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 50, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("x"), 40)); err != nil {
		t.Fatal(err)
	}
	// Read-only directory: rename fails, but the existing active file can
	// still be opened for append and written.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatalf("write during rotate failure should keep the record: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 60 {
		t.Fatalf("active file = %v (%v), want 60 bytes appended past maxSize", info, err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatal(err)
	}
	rots, err := scanRotations(dir, "goose.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(rots) != 1 || rots[0].size != 60 {
		t.Fatalf("rotations after recovery = %+v, want one 60-byte rotation", rots)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 20 {
		t.Fatalf("active file after recovery = %v (%v), want 20", info, err)
	}
}

// TestWriterBrokenRotationStaysWithinBudget: while rotation keeps failing,
// the active file may pass maxSize but must never pass maxTotal — writes
// that would breach the total budget are dropped (stderr keeps them).
func TestWriterBrokenRotationStaysWithinBudget(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not constrain root")
	}
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 50, 120)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("x"), 40)
	for i := 0; i < 20; i++ {
		_, _ = w.Write(chunk) // some writes must be dropped once the budget is reached
		if info, err := os.Stat(path); err != nil || info.Size() > 120 {
			t.Fatalf("write %d: active file %v (%v) exceeds total budget 120", i, info, err)
		}
	}
}

// TestWriterRotateNilHandleNoPanic: rename fails AND the reopen fails (file
// read-only, directory read-only), leaving f=nil with a stale size. The next
// Write must return an error, not panic closing a nil handle, and must
// recover once permissions are restored.
func TestWriterRotateNilHandleNoPanic(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("file permissions do not constrain root")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	t.Cleanup(func() {
		_ = os.Chmod(path, 0o644)
		_ = os.Chmod(dir, 0o755)
	})
	w, err := NewWriter(path, 50, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("x"), 40)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	// Rename fails; reopening the read-only file fails too: f=nil.
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err == nil {
		t.Fatal("write with no recoverable handle should fail")
	}
	// Second write re-enters rotate with f=nil: must error, not panic.
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err == nil {
		t.Fatal("write with nil handle should fail")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	rots, err := scanRotations(dir, "goose.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(rots) == 0 {
		t.Fatal("writer never recovered after permissions were restored")
	}
}

// TestWriterNoFDLeakWhileRotationBroken: with rotation persistently failing
// (read-only dir) but the active file still appendable, every record retries
// the rotation. That retry must not leak a descriptor per write — sustained
// logging would otherwise exhaust the process fd table.
func TestWriterNoFDLeakWhileRotationBroken(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not constrain root")
	}
	countFDs := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Skip("no /proc/self/fd on this platform")
		}
		return len(entries)
	}
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 50, 100000)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("x"), 40)); err != nil {
		t.Fatal(err)
	}
	before := countFDs()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := w.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if after := countFDs(); after > before+2 {
		t.Fatalf("fd leak while rotation broken: %d -> %d", before, after)
	}
}

func TestWriterPurge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 10, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte("0123456789")); err != nil {
			t.Fatal(err)
		}
	}
	other := filepath.Join(dir, "unrelated.log")
	if err := os.WriteFile(other, []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := w.Purge(); err != nil {
		t.Fatal(err)
	}
	if got := footprint(t, dir, "goose.log"); got != 0 {
		t.Fatalf("footprint after purge = %d, want 0", got)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("unrelated file was purged: %v", err)
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("write after purge should fail (writer closed)")
	}
}

// TestWriterPruneCountsActiveOnce pins the prune accounting: pending is only
// the incoming record, never the active size again. With the double-count
// bug this scenario deletes a second rotation that still fits the budget.
func TestWriterPruneCountsActiveOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.log")
	w, err := NewWriter(path, 100, 250)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	chunk := bytes.Repeat([]byte("x"), 60)
	for i := 0; i < 4; i++ { // active 60 + three 60-byte rotations = 240 <= 250
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if got := footprint(t, dir, "goose.log"); got != 240 {
		t.Fatalf("footprint before trigger = %d, want 240", got)
	}
	// No rotation this write (60+40 <= 100), but 60+40+180 > 250: exactly one
	// 60-byte rotation must be pruned, leaving 100 + 120 = 220.
	if _, err := w.Write(bytes.Repeat([]byte("x"), 40)); err != nil {
		t.Fatal(err)
	}
	if got := footprint(t, dir, "goose.log"); got != 220 {
		t.Fatalf("footprint after prune = %d, want 220 (active counted twice?)", got)
	}
	rots, err := w.scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(rots) != 2 {
		t.Fatalf("rotations after prune = %d, want 2", len(rots))
	}
}