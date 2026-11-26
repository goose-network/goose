// Package logfile implements the engine's bounded log storage: a rotating
// file writer whose active file plus retained rotations never exceed a total
// byte budget, and a manager that fans the standard logger out to stderr and
// the current writer with hot reconfiguration.
package logfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// tsLayout is the timestamp embedded in rotated file names. It sorts
// lexicographically in chronological order and parses back unambiguously, so
// pruning can identify this writer's own rotations — and only those — in a
// shared directory.
const tsLayout = "20060102T150405.000000000Z"

// rotNameOverhead is the bytes a rotation name adds to the active base name:
// the '-' separator plus the fixed-width timestamp (len of the layout's
// formatted form).
const rotNameOverhead = 1 + len("20060102T150405.000000000Z")

// nameMax is the per-component file name limit on every filesystem goose
// targets (Linux NAME_MAX). A base name whose rotation form would exceed it
// is rejected up front: os.Stat on an over-long name returns ENAMETOOLONG
// forever, which must never be mistaken for a name collision.
const nameMax = 255

// Writer is an io.WriteCloser that appends to an active log file, rotates it
// once it reaches maxSize bytes, and deletes the oldest rotated files whenever
// the total footprint (active + rotations) would exceed maxTotal bytes. The
// budget holds at every write, not just at rotation: a write that would push
// the total over prunes first. maxSize is clamped to maxTotal, so the active
// file alone can never exceed the budget.
//
// Only files this writer recognizes — the active path and stem-<timestamp>.ext
// rotations whose timestamp parses — are ever deleted; unrelated files sharing
// the directory are left alone.
type Writer struct {
	mu       sync.Mutex
	path     string
	dir      string
	stem     string
	ext      string
	maxSize  int64
	maxTotal int64
	f        *os.File
	size     int64 // bytes in the active file
	archived int64 // bytes in recognized rotations, refreshed on rotate/prune
	broken   bool  // last rotate/reopen failed; Write retries the open
	closed   bool
}

// NewWriter opens (or creates) the active log file at path and enforces the
// byte budget immediately: pre-existing rotations — e.g. left by a previous
// run — count toward maxTotal and the oldest are pruned until the total fits.
// An active file already at or over maxSize is rotated first so it can be
// pruned too. maxTotal must be positive; maxSize is clamped into (0, maxTotal].
func NewWriter(path string, maxSize, maxTotal int64) (*Writer, error) {
	if maxTotal <= 0 {
		return nil, errors.New("logfile: maxTotal must be positive")
	}
	if maxSize <= 0 || maxSize > maxTotal {
		maxSize = maxTotal
	}
	base := filepath.Base(path)
	if len(base)+rotNameOverhead > nameMax {
		return nil, fmt.Errorf("logfile: base name %q too long: rotation names would exceed the %d-byte file name limit", base, nameMax)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logfile: mkdir %s: %w", dir, err)
	}
	ext := filepath.Ext(base)
	w := &Writer{
		path:     path,
		dir:      dir,
		stem:     strings.TrimSuffix(base, ext),
		ext:      ext,
		maxSize:  maxSize,
		maxTotal: maxTotal,
	}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	if w.size >= maxSize {
		if err := w.rotate(); err != nil {
			_ = w.f.Close()
			return nil, err
		}
	}
	if err := w.prune(0); err != nil {
		_ = w.f.Close()
		return nil, err
	}
	return w, nil
}

// openActive opens (or creates) the active file in append mode and adopts its
// on-disk size. Any previous handle is closed first so a reopen can never
// leak a descriptor. The caller holds w.mu (or is constructing the Writer).
func (w *Writer) openActive() error {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logfile: open %s: %w", w.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logfile: stat %s: %w", w.path, err)
	}
	w.f = f
	w.size = info.Size()
	w.broken = false
	return nil
}

// Write appends p to the active file, rotating and pruning as needed to keep
// both the per-file size and the total footprint within budget. A record
// larger than maxSize is rejected whole rather than split across files.
//
// A failed rotation never wedges the writer: the active file is reopened on
// the next Write (self-healing once the transient filesystem cause clears),
// and while the open keeps failing the record is still accepted into whatever
// handle remains rather than lost — stderr always carries the full stream.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("logfile: write on closed writer")
	}
	n := int64(len(p))
	if n > w.maxSize {
		return 0, fmt.Errorf("logfile: write length %d exceeds max file size %d", n, w.maxSize)
	}
	if w.f == nil {
		// A previous rotate/reopen failure left no handle. Retry the open;
		// if the cause persists the error surfaces per write instead of
		// wedging silently. A live handle is never replaced here: reopening
		// over it would leak a descriptor per record while rotation stays
		// broken — rotation itself retries on the next oversized write.
		_ = w.openActive()
	}
	if w.size > 0 && w.size+n > w.maxSize {
		if err := w.rotate(); err != nil {
			// Keep the record if the budget allows: appending past maxSize
			// for one write is preferable to dropping it, and the next Write
			// retries the rotation. But if rotation is broken and appending
			// would breach the TOTAL budget, drop the file copy (stderr
			// always carries the full stream) — an unrotatable active file
			// must not grow without bound.
			w.broken = true
			if w.f == nil || w.size+n+w.archived > w.maxTotal {
				return 0, err
			}
		}
	}
	if w.size+n+w.archived > w.maxTotal {
		if err := w.prune(n); err != nil {
			return 0, err
		}
	}
	if w.f == nil {
		return 0, fmt.Errorf("logfile: %s unavailable", w.path)
	}
	nn, err := w.f.Write(p)
	w.size += int64(nn)
	return nn, err
}

// Close closes the active file. Rotated files stay on disk within the budget.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

// Purge removes every file this writer owns — the active file and all
// recognized rotations — and marks it closed. The Manager calls it when a
// live path change retires a writer, so the old location's logs cannot
// orphan outside the new writer's budget.
func (w *Writer) Purge() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.f != nil {
		_ = w.f.Close()
	}
	return purgeLocation(w.path)
}

// canonicalPath renders a log path as an absolute cleaned form so aliased
// spellings of one location ("dir/./a.log" vs "dir/a.log", relative vs
// absolute) compare equal — the Manager must not mistake an alias for a path
// change and purge the live writer's files.
func canonicalPath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return filepath.Clean(path)
}

// purgeLocation removes the active file and every recognized rotation at
// path without needing a live Writer — the Manager uses it to retire a
// location whose writer was already closed (file logging disabled, then
// re-enabled elsewhere). Unrelated files are never touched. It keeps going
// past individual removal failures and returns their joined error, so one
// unwritable file does not strand the rest of the location.
func purgeLocation(path string) error {
	dir, base := filepath.Dir(path), filepath.Base(path)
	rots, err := scanRotations(dir, base)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range rots {
		if err := os.Remove(filepath.Join(dir, r.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("logfile: purge %s: %w", r.name, err))
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("logfile: purge %s: %w", path, err))
	}
	return errors.Join(errs...)
}

// rotate closes the active file, renames it to a timestamped rotation and
// opens a fresh active file. On any failure the writer is left recoverable:
// the active path is reopened (best effort) so the next Write can retry the
// rotation instead of dying on a closed handle forever. The caller holds w.mu.
func (w *Writer) rotate() error {
	if w.f == nil {
		// A previous failure left no handle (rename failed AND the reopen
		// failed). Recover one first — closing a nil handle would panic.
		if err := w.openActive(); err != nil {
			return err
		}
	}
	// Nanosecond timestamps make collisions practically impossible; a
	// backwards clock step is the only real case, handled by bumping the
	// stamp until the name is free (an overwrite would silently lose a
	// rotation's worth of logs). Any Stat error other than NotExist is a
	// real failure (EACCES, ENAMETOOLONG, ...) — never a collision: bumping
	// the fixed-width stamp cannot clear it, and looping would spin forever
	// holding the lock every logger in the process needs.
	ts := time.Now().UTC()
	var dst string
	for {
		dst = filepath.Join(w.dir, w.stem+"-"+ts.Format(tsLayout)+w.ext)
		_, err := os.Stat(dst)
		if errors.Is(err, fs.ErrNotExist) {
			break
		}
		if err != nil {
			return fmt.Errorf("logfile: rotate stat %s: %w", dst, err)
		}
		ts = ts.Add(time.Nanosecond)
	}
	if err := w.f.Close(); err != nil {
		w.f = nil
		w.broken = true
		return fmt.Errorf("logfile: close %s: %w", w.path, err)
	}
	if err := os.Rename(w.path, dst); err != nil {
		// The active file was not renamed; reopen it so writes continue
		// and the next rotation attempt can succeed.
		w.f = nil
		w.broken = true
		_ = w.openActive()
		return fmt.Errorf("logfile: rotate %s: %w", w.path, err)
	}
	// The rename succeeded: the old active bytes are now a rotation.
	// Account for them before the reopen so a failed open cannot drift.
	w.archived += w.size
	w.size = 0
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		w.f = nil
		w.broken = true
		return fmt.Errorf("logfile: reopen %s: %w", w.path, err)
	}
	w.f = f
	w.broken = false
	return nil
}

// rotation is one recognized rotated file on disk.
type rotation struct {
	name string
	size int64
	when time.Time
}

// scan lists the recognized rotations in the directory, oldest first. It
// re-reads sizes from disk so external changes (a deleted or added file) are
// reflected in the next prune. The caller holds w.mu.
func (w *Writer) scan() ([]rotation, error) {
	return scanRotations(w.dir, filepath.Base(w.path))
}

// scanRotations lists the rotations belonging to base (stem-<timestamp>.ext
// with a parseable timestamp) in dir, oldest first.
func scanRotations(dir, base string) ([]rotation, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("logfile: read dir %s: %w", dir, err)
	}
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	var rots []rotation
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == base || !strings.HasPrefix(name, stem+"-") || !strings.HasSuffix(name, ext) {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, stem+"-"), ext)
		when, err := time.Parse(tsLayout, mid)
		if err != nil {
			continue // not one of ours (e.g. goose-notes.log) — never touched
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		rots = append(rots, rotation{name: name, size: info.Size(), when: when})
	}
	sort.Slice(rots, func(i, j int) bool {
		if rots[i].when.Equal(rots[j].when) {
			return rots[i].name < rots[j].name
		}
		return rots[i].when.Before(rots[j].when)
	})
	return rots, nil
}

// prune deletes the oldest rotations until the total footprint — active file
// plus pending bytes about to be written plus rotations — fits maxTotal.
// pending is ONLY the incoming record's length: the active file's current
// size is already counted via w.size. The active file is never deleted; with
// maxSize <= maxTotal and pending <= maxSize the budget is always reachable.
// The caller holds w.mu.
func (w *Writer) prune(pending int64) error {
	rots, err := w.scan()
	if err != nil {
		return err
	}
	total := w.size + pending
	for _, r := range rots {
		total += r.size
	}
	w.archived = total - w.size - pending
	for _, r := range rots {
		if total <= w.maxTotal {
			break
		}
		if err := os.Remove(filepath.Join(w.dir, r.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("logfile: prune %s: %w", r.name, err)
		}
		total -= r.size
		w.archived -= r.size
	}
	return nil
}