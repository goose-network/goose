// persist.go — durable configuration: the on-disk document shape, the
// snapshot that excludes provider-regenerated state, and a write-through
// persister that keeps the file in sync with API edits.
//
// The store is the single source of truth for the engine, and every admin
// API mutation goes through its Set*/Delete* accessors, which bump the
// config version. The Persister subscribes to those bumps and writes the
// snapshot back to the file the engine was started with (-config), so edits
// made through the REST API survive restarts. Provider-managed outbounds are
// NOT persisted (they are regenerated from the provider specs on the next
// poll — a persisted copy would go stale as the server list changes), and
// provider-managed pools are persisted without their OutboundIDs: the pool
// object itself (and the operator's Filters/Selector/Access tuning) is
// operator state, its membership is provider output.

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Doc is the on-disk config document: the same shape loadConfig reads, so a
// file this package writes can be fed straight back into a restart.
type Doc struct {
	Engine    Engine          `json:"engine"`
	Inbounds  []*Inbound      `json:"inbounds"`
	Outbounds []*OutboundSpec `json:"outbounds"`
	Pools     []*Pool         `json:"pools"`
	Chains    []*ChainSpec    `json:"chains"`
	Providers []*ProviderSpec `json:"providers"`
}

// Snapshot returns the persistable document under one lock: the engine
// config plus every operator object, sorted by ID so consecutive snapshots
// diff cleanly. Provider-managed outbounds are skipped, and provider-managed
// pools keep their id/filters/selector/access but drop OutboundIDs (see the
// package comment).
func (s *Store) Snapshot() Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()

	managed := map[string]struct{}{}
	for _, owned := range s.managedOutbounds {
		for id := range owned {
			managed[id] = struct{}{}
		}
	}

	doc := Doc{Engine: s.engine}
	for _, in := range s.inbounds {
		doc.Inbounds = append(doc.Inbounds, in)
	}
	for id, o := range s.outbounds {
		if _, isManaged := managed[id]; isManaged {
			continue
		}
		doc.Outbounds = append(doc.Outbounds, o)
	}
	for id, p := range s.pools {
		if _, isManaged := s.managedPools[id]; isManaged {
			stripped := *p
			stripped.OutboundIDs = nil
			doc.Pools = append(doc.Pools, &stripped)
			continue
		}
		doc.Pools = append(doc.Pools, p)
	}
	for _, c := range s.chains {
		doc.Chains = append(doc.Chains, c)
	}
	for _, p := range s.providers {
		doc.Providers = append(doc.Providers, p)
	}

	sort.Slice(doc.Inbounds, func(i, j int) bool { return doc.Inbounds[i].ID < doc.Inbounds[j].ID })
	sort.Slice(doc.Outbounds, func(i, j int) bool { return doc.Outbounds[i].ID < doc.Outbounds[j].ID })
	sort.Slice(doc.Pools, func(i, j int) bool { return doc.Pools[i].ID < doc.Pools[j].ID })
	sort.Slice(doc.Chains, func(i, j int) bool { return doc.Chains[i].ID < doc.Chains[j].ID })
	sort.Slice(doc.Providers, func(i, j int) bool { return doc.Providers[i].ID < doc.Providers[j].ID })
	return doc
}

// WriteDoc marshals doc and replaces path atomically (temp file + rename),
// so a crash mid-write never leaves a torn config.
func WriteDoc(path string, doc Doc) error {
	data, err := marshalDoc(doc)
	if err != nil {
		return err
	}
	return writeFile(path, data)
}

// marshalDoc renders doc the way every persisted config is written: indented
// JSON with a trailing newline.
func marshalDoc(doc Doc) ([]byte, error) {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("config: marshal doc: %w", err)
	}
	return append(data, '\n'), nil
}

// writeFile replaces path with data atomically.
func writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("config: temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("config: write %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("config: close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("config: rename onto %s: %w", path, err)
	}
	return nil
}

// Persister keeps a config file in sync with the store. Every version bump
// schedules a write, debounced so a burst of mutations (a config load, an
// engine reconcile) costs one write; a snapshot that is byte-identical to
// the last one skips the I/O entirely, so a provider's poll cadence does not
// rewrite the file forever.
type Persister struct {
	store *Store
	path  string

	mu        sync.Mutex
	lastWrite []byte  // last bytes written; identical snapshots skip the I/O
	writes    int     // actual write count (for tests)
	version   <-chan uint64
	stop      chan struct{}
	done      chan struct{}
}

// NewPersister returns a Persister for store → path. It does nothing until
// Run is called.
func NewPersister(store *Store, path string) *Persister {
	return &Persister{
		store:   store,
		path:    path,
		version: store.Subscribe(),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Run writes the current snapshot once (normalizing the file to what the
// engine believes, e.g. after a hand-edited config was loaded), then on
// every version bump, debounced by the given coalescing window. It blocks
// until Stop is called; Stop performs the final flush.
func (p *Persister) Run(debounce time.Duration) {
	defer close(p.done)
	p.persist()
	timer := time.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C
	}
	armed := false
	for {
		select {
		case <-p.stop:
			timer.Stop()
			p.persist()
			return
		case _, ok := <-p.version:
			if !ok {
				p.persist()
				return
			}
			// (Re)arm the debounce so bursts collapse into one write.
			if armed && !timer.Stop() {
				<-timer.C
			}
			timer.Reset(debounce)
			armed = true
		case <-timer.C:
			armed = false
			p.persist()
		}
	}
}

// persist snapshots, writes, and logs failures without stopping the loop —
// a temporarily unwritable path must not take the engine's config bus down.
func (p *Persister) persist() {
	data, err := marshalDoc(p.store.Snapshot())
	if err != nil {
		log.Printf("config: persist %s: %v", p.path, err)
		return
	}
	p.mu.Lock()
	unchanged := bytes.Equal(p.lastWrite, data)
	p.mu.Unlock()
	if unchanged {
		return
	}
	if err := writeFile(p.path, data); err != nil {
		log.Printf("config: persist %s: %v", p.path, err)
		return
	}
	p.mu.Lock()
	p.lastWrite = data
	p.writes++
	p.mu.Unlock()
}

// Stop terminates the run loop and flushes once more, so a shutdown right
// after an edit is not lost to the debounce window. It blocks until the
// flush finished and is safe to call more than once.
func (p *Persister) Stop() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}

// writeCount reports how many times the file was actually written so far.
func (p *Persister) writeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writes
}
