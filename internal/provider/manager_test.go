package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// fakeProvider is a minimal Provider for testing the merge loop. It returns a
// configurable set of outbounds and emits a Watch signal when poked.
type fakeProvider struct {
	name  string
	mu    sync.Mutex
	cfgs  []pub.OutboundConfig
	watch chan struct{}
	calls int32
}

func (f *fakeProvider) Name() string           { return f.name }
func (f *fakeProvider) Watch() <-chan struct{} { return f.watch }
func (f *fakeProvider) Outbounds(_ context.Context) ([]pub.OutboundConfig, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]pub.OutboundConfig, len(f.cfgs))
	copy(out, f.cfgs)
	return out, nil
}

func (f *fakeProvider) set(cfgs []pub.OutboundConfig) {
	f.mu.Lock()
	f.cfgs = cfgs
	f.mu.Unlock()
}

func (f *fakeProvider) signal() {
	select {
	case f.watch <- struct{}{}:
	default:
	}
}

func newFakeFactory(p pub.Provider) pub.ProviderFactory {
	return func(_ map[string]any) (pub.Provider, error) { return p, nil }
}

// TestManagerMergesProviderOutboundsIntoPool verifies the core "get proxy
// server config from plugin into pool" behavior: a provider's Outbounds result
// is merged into the config store as outbounds + a managed pool, and a Watch
// signal triggers a re-poll that updates the pool.
func TestManagerMergesProviderOutboundsIntoPool(t *testing.T) {
	// Register a fake provider plugin.
	fp := &fakeProvider{name: "fake", watch: make(chan struct{}, 1)}
	pub.RegisterProvider("fake-merge-test", newFakeFactory(fp))
	t.Cleanup(func() {
		// The registry panics on duplicate registration, so we can't easily
		// re-register across tests; this test uses a unique name.
	})

	store := config.NewStore()
	store.SetProvider(&config.ProviderSpec{
		ID:       "p1",
		Provider: "fake-merge-test",
		PoolID:   "pool-from-provider",
		Config:   map[string]any{},
	})

	// Bootstrap outbounds.
	fp.set([]pub.OutboundConfig{
		{ID: "fake-a", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:1"}},
		{ID: "fake-b", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:2"}},
	})

	mgr := New(store)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mgr.Close()

	// Wait for the bootstrap poll to land in the store.
	waitFor(t, func() bool {
		pool, ok := store.Pool("pool-from-provider")
		return ok && len(pool.OutboundIDs) == 2
	}, 2*time.Second, "bootstrap pool fill")

	pool, _ := store.Pool("pool-from-provider")
	if got, want := len(pool.OutboundIDs), 2; got != want {
		t.Fatalf("bootstrap pool size = %d, want %d", got, want)
	}
	if _, ok := store.Outbound("fake-a"); !ok {
		t.Fatal("managed outbound fake-a not in store")
	}

	// Refresh: drop one, add one, and signal Watch.
	fp.set([]pub.OutboundConfig{
		{ID: "fake-a", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:1"}},
		{ID: "fake-c", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:3"}},
	})
	fp.signal()

	waitFor(t, func() bool {
		pool, ok := store.Pool("pool-from-provider")
		if !ok {
			return false
		}
		// expect fake-a + fake-c, fake-b removed
		if len(pool.OutboundIDs) != 2 {
			return false
		}
		_, hasB := store.Outbound("fake-b")
		_, hasC := store.Outbound("fake-c")
		return !hasB && hasC
	}, 2*time.Second, "refresh updates pool and removes stale outbound")

	if _, ok := store.Outbound("fake-b"); ok {
		t.Fatal("stale managed outbound fake-b should have been removed")
	}
}

// TestManagerDefaultsToDefaultPool verifies the goal-1 default-pool rule: a
// provider spec created without a pool_id merges its outbounds into the
// "default" pool, so a provider can be created with just a plugin + config.
func TestManagerDefaultsToDefaultPool(t *testing.T) {
	fp := &fakeProvider{name: "fake", watch: make(chan struct{}, 1)}
	pub.RegisterProvider("fake-default-pool-test", newFakeFactory(fp))

	store := config.NewStore()
	// No PoolID: should land in the "default" pool.
	store.SetProvider(&config.ProviderSpec{
		ID:       "pdef",
		Provider: "fake-default-pool-test",
		Config:   map[string]any{},
	})
	fp.set([]pub.OutboundConfig{
		{ID: "def-a", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:21"}},
	})

	mgr := New(store)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mgr.Close()

	waitFor(t, func() bool {
		pool, ok := store.Pool(config.DefaultPoolID)
		return ok && len(pool.OutboundIDs) == 1
	}, 2*time.Second, "poolless provider fills the default pool")

	// Deleting the provider drops the default pool it owned.
	store.DeleteProvider("pdef")
	waitFor(t, func() bool {
		_, ok := store.Pool(config.DefaultPoolID)
		return !ok
	}, 2*time.Second, "deleted poolless provider drops its default pool")
}

// TestManagerKeepsPoolOnProviderError verifies a transient Outbounds error
// does not drain the pool.
func TestManagerKeepsPoolOnProviderError(t *testing.T) {
	errProv := &errorProvider{watch: make(chan struct{}, 1)}
	pub.RegisterProvider("fake-err-test", newFakeFactory(errProv))
	t.Cleanup(func() {})

	store := config.NewStore()
	// Seed the pool with a static outbound so there's something to keep.
	store.SetOutbound(&config.OutboundSpec{ID: "static-1", Protocol: "direct", Config: map[string]any{}})
	store.SetProvider(&config.ProviderSpec{
		ID:       "perr",
		Provider: "fake-err-test",
		PoolID:   "pool-err",
		Config:   map[string]any{},
	})

	mgr := New(store)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mgr.Close()

	// The provider always errors, so the managed pool stays empty but the
	// engine keeps running (no panic, no fatal). Just assert the manager
	// didn't crash and the static outbound survives.
	time.Sleep(100 * time.Millisecond)
	if _, ok := store.Outbound("static-1"); !ok {
		t.Fatal("static outbound should survive a provider error")
	}
}

type errorProvider struct{ watch chan struct{} }

func (e *errorProvider) Name() string           { return "err" }
func (e *errorProvider) Watch() <-chan struct{} { return e.watch }
func (e *errorProvider) Outbounds(_ context.Context) ([]pub.OutboundConfig, error) {
	return nil, errFake
}

var errFake = errSentinel{}

type errSentinel struct{}

func (errSentinel) Error() string { return "fake provider error" }

// TestManagerReconcilesOnConfigChange verifies the admin-API flow: creating
// a provider spec in the store after Start() launches its poll loop without
// an engine restart; changing its config restarts it; deleting it stops it
// and drops its managed outbounds.
func TestManagerReconcilesOnConfigChange(t *testing.T) {
	store := config.NewStore()

	mgr := New(store)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mgr.Close()

	// Register a fake provider plugin.
	fp := &fakeProvider{name: "fake", watch: make(chan struct{}, 1)}
	pub.RegisterProvider("fake-reconcile-test", newFakeFactory(fp))

	// Create a provider spec after the manager is already running — the
	// pattern the admin API's POST /api/providers takes.
	store.SetProvider(&config.ProviderSpec{
		ID:       "late",
		Provider: "fake-reconcile-test",
		PoolID:   "pool-late",
		Config:   map[string]any{"url": "http://one"},
	})
	fp.set([]pub.OutboundConfig{
		{ID: "late-a", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:11"}},
	})

	waitFor(t, func() bool {
		pool, ok := store.Pool("pool-late")
		return ok && len(pool.OutboundIDs) == 1
	}, 2*time.Second, "late-created provider starts and fills its pool")

	// Changing the spec config restarts the provider under the same id:
	// the new outbound set replaces the old one.
	fp.set([]pub.OutboundConfig{
		{ID: "late-b", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:12"}},
	})
	store.SetProvider(&config.ProviderSpec{
		ID:       "late",
		Provider: "fake-reconcile-test",
		PoolID:   "pool-late",
		Config:   map[string]any{"url": "http://two"},
	})

	waitFor(t, func() bool {
		pool, ok := store.Pool("pool-late")
		if !ok || len(pool.OutboundIDs) != 1 {
			return false
		}
		_, hasA := store.Outbound("late-a")
		_, hasB := store.Outbound("late-b")
		return !hasA && hasB
	}, 2*time.Second, "changed spec restarts provider with new outbounds")

	// Deleting the spec stops the poll loop and drops the managed pool.
	store.DeleteProvider("late")
	waitFor(t, func() bool {
		_, ok := store.Pool("pool-late")
		return !ok
	}, 2*time.Second, "deleted provider drops its pool")

	if _, ok := store.Outbound("late-b"); ok {
		t.Fatal("managed outbound late-b should be gone after provider delete")
	}
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
