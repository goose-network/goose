package logfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Manager is the standard logger's output: every record goes to stderr and,
// when a log file is configured, to a bounded rotating Writer as well. It is
// safe for concurrent use, and Reconfigure swaps the file writer live so a
// config edit (PUT /api/engine) takes effect without a restart.
//
// The zero Manager logs to stderr only. Reconfigure("", ...) returns it to
// that state; a writer that fails to open leaves the previous output in place
// — an unwritable log path must not take the engine down, mirroring the
// config persister's philosophy.
//
// Locations pending deletion (a moved-away path, a purge that failed) are
// recorded in a ledger file beside the active log, so a crash or restart
// cannot orphan them outside the budget: the next Reconfigure replays the
// ledger. The ledger lives in the active file's directory, which the writer
// already owns and creates.
type Manager struct {
	mu       sync.Mutex
	w        *Writer
	path     string // canonical active path; "" when file logging is off
	pending  []string
	maxSize  int64
	maxTotal int64
	closed   bool      // Close retires the Manager: Reconfigure becomes a no-op
	lastWarn time.Time // rate-limits file-write warnings written straight to stderr
}

// NewManager returns a stderr-only Manager.
func NewManager() *Manager { return &Manager{} }

// Reconfigure points the file output at path with the given byte budgets.
// Identical parameters are a no-op, so calling it on every config version
// bump is cheap. A failed open keeps the previous writer and warns on stderr.
//
// The whole swap runs under m.mu: while the new Writer is being constructed
// (which may rotate and prune the SAME active path the old writer owns), no
// record can flow through the old writer — two writers appending to one inode
// would corrupt both the budget accounting and the log history. On a path
// change the retired writer's files are purged: leaving them behind would
// orphan up to a full budget of goose-owned logs outside anyone's accounting,
// breaking the aggregate cap across repeated path edits. Disabling file
// logging keeps the logs for inspection but remembers the location in the
// ledger, so re-enabling elsewhere purges it too — even after a restart.
func (m *Manager) Reconfigure(path string, maxSize, maxTotal int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		// Retired by Close: a config-watch iteration racing shutdown must
		// not recreate the log file with a writer nothing will ever close.
		return
	}
	if path != "" {
		path = canonicalPath(path)
	}
	// Already disabled: a repeat Reconfigure("") (the watch loop passes the
	// resolved positive budgets even for "-") must be a true no-op — it must
	// not re-enter the disable branch and disturb the retirement ledger.
	if path == "" && m.path == "" && m.w == nil {
		return
	}
	if m.path == path && m.maxSize == maxSize && m.maxTotal == maxTotal && (m.w != nil || path == "") {
		return
	}
	if path == "" {
		old, oldPath := m.w, m.path
		if old != nil {
			_ = old.Close()
		}
		if oldPath != "" {
			// Retain the logs for inspection but remember the location so a
			// later move (in this process or the next) purges them. Repeat
			// disables must not erase the record: oldPath is "" then. The
			// ledger is written beside oldPath (m.path is still oldPath here),
			// where the next start will look for it.
			m.addPendingLocked(oldPath)
		}
		m.w, m.path, m.maxSize, m.maxTotal = nil, "", 0, 0
		return
	}
	old, oldPath := m.w, m.path
	// The new Writer is constructed while the old handle is still open: no
	// record can flow through either (m.mu is held), and a failed open must
	// leave the previous writer usable. On success the old handle is closed
	// (same path) or purged (path change) below.
	w, err := NewWriter(path, maxSize, maxTotal)
	if err != nil {
		// Straight to stderr, not through the standard logger: that logger
		// writes through this Manager, and warning about a broken file
		// output must not depend on the file output.
		fmt.Fprintf(os.Stderr, "logfile: %v (keeping previous log output)\n", err)
		return
	}
	m.w, m.path, m.maxSize, m.maxTotal = w, path, maxSize, maxTotal
	// Replay retirements recorded by an earlier process (disable-then-restart,
	// crash mid-swap) before adding this swap's own retirement.
	m.loadPendingLocked()
	if old != nil {
		if oldPath != path {
			// Record the retirement BEFORE purging: a crash between the two
			// must leave a durable trace the next start can replay.
			m.addPendingLocked(oldPath)
			_ = old.Purge()
		} else {
			_ = old.Close()
		}
	}
	m.sweepPendingLocked()
}

// ledgerPath is the retirement ledger for the active location.
func ledgerPath(path string) string {
	return filepath.Join(filepath.Dir(path), ".goose-log-retired")
}

// addPendingLocked records a location awaiting purge in memory and persists
// the ledger at the active location immediately, so the record survives a
// crash before the purge runs.
func (m *Manager) addPendingLocked(path string) {
	for _, p := range m.pending {
		if p == path {
			return
		}
	}
	m.pending = append(m.pending, path)
	m.writeLedgerLocked()
}

// loadPendingLocked merges the on-disk ledger into memory. The ledger may
// hold records written by an earlier process (disable-then-restart, or a
// crash mid-swap) that this Manager has never seen.
func (m *Manager) loadPendingLocked() {
	if m.path == "" {
		return
	}
	data, err := os.ReadFile(ledgerPath(m.path))
	if err != nil {
		return
	}
	for _, line := range splitLines(string(data)) {
		if line == "" || line == m.path {
			continue
		}
		dup := false
		for _, p := range m.pending {
			if p == line {
				dup = true
				break
			}
		}
		if !dup {
			m.pending = append(m.pending, line)
		}
	}
}

// sweepPendingLocked purges every remembered location except the active one.
// Failures keep the record (and warn) so a later Reconfigure retries once the
// filesystem heals; successes drop it and rewrite the ledger.
func (m *Manager) sweepPendingLocked() {
	if m.path == "" {
		return
	}
	var keep []string
	for _, p := range m.pending {
		if p == m.path {
			continue // never purge the live location (aliased spellings)
		}
		if err := purgeLocation(p); err != nil {
			fmt.Fprintf(os.Stderr, "logfile: purge retired log location: %v (will retry)\n", err)
			keep = append(keep, p)
		}
	}
	if len(keep) != len(m.pending) {
		m.pending = keep
		m.writeLedgerLocked()
	}
}

// writeLedgerLocked persists the pending list beside the active location,
// removing the ledger when it empties. A ledger write failure only warns: the
// in-memory list still drives this process's sweeps.
func (m *Manager) writeLedgerLocked() {
	if m.path != "" {
		m.writeLedgerAtLocked(m.path)
	}
}

// writeLedgerAtLocked writes the pending list to the ledger beside dir's
// location — used with the OLD path on disable, when there is no active
// location anymore but the next start must still find the record.
func (m *Manager) writeLedgerAtLocked(location string) {
	lp := ledgerPath(location)
	if len(m.pending) == 0 {
		if err := os.Remove(lp); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "logfile: remove ledger %s: %v\n", lp, err)
		}
		return
	}
	var data []byte
	for _, p := range m.pending {
		data = append(data, p...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(lp, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "logfile: write ledger %s: %v\n", lp, err)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// Write implements io.Writer. Stderr is written first so console/container
// output never depends on file health; the file copy is best-effort, with
// failures warned on stderr at most once a minute (again bypassing the
// standard logger to avoid recursing through a broken output).
func (m *Manager) Write(p []byte) (int, error) {
	n, err := os.Stderr.Write(p)
	m.mu.Lock()
	w := m.w
	if w != nil {
		if _, werr := w.Write(p); werr != nil && time.Since(m.lastWarn) > time.Minute {
			m.lastWarn = time.Now()
			fmt.Fprintf(os.Stderr, "logfile: write: %v\n", werr)
		}
	}
	m.mu.Unlock()
	return n, err
}

// Close retires the Manager: the file writer is closed and Reconfigure
// becomes a no-op, so a config-watch iteration racing shutdown cannot
// resurrect a file writer nothing would ever close. Write keeps working as
// a stderr-only output afterwards. The retirement ledger stays on disk for
// the next process to replay.
func (m *Manager) Close() {
	m.mu.Lock()
	old := m.w
	m.w, m.path, m.maxSize, m.maxTotal = nil, "", 0, 0
	m.closed = true
	m.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}