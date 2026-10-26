package provider

import (
	"testing"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// TestSeedDefaultSeedsDeclaredDefault verifies the plugin-agnostic seeding:
// a store with no providers gets the provider a plugin declared through the
// registry; which plugin declared itself is invisible to the engine. It also
// registers the single default declaration this package's tests share (the
// registry is process-wide and panics on a second declaration).
func TestSeedDefaultSeedsDeclaredDefault(t *testing.T) {
	pub.RegisterDefaultProvider(pub.DefaultProvider{
		Provider: "fake-default-seed-test",
		Config:   map[string]any{"some": "default"},
	})

	store := config.NewStore()
	if got := SeedDefault(store); !got {
		t.Fatal("SeedDefault on an empty store should seed the declared default")
	}
	spec, ok := store.Provider("fake-default-seed-test")
	if !ok {
		t.Fatal("seeded default provider not in store")
	}
	if spec.Provider != "fake-default-seed-test" {
		t.Fatalf("spec.Provider = %q, want the declared provider name", spec.Provider)
	}
	if v, _ := spec.Config["some"].(string); v != "default" {
		t.Fatalf("spec.Config propagated incorrectly: %v", spec.Config)
	}
	// EffectivePoolIDs defaults an unset pool list to the default pool.
	if got := spec.EffectivePoolIDs(); len(got) != 1 || got[0] != config.DefaultPoolID {
		t.Fatalf("seeded spec pools = %v, want [%q]", got, config.DefaultPoolID)
	}

	// Seeding is once-only: a second call against the now-populated store
	// must not add anything (an explicit provider list opts out).
	if got := SeedDefault(store); got {
		t.Fatal("second SeedDefault on a populated store should be a no-op")
	}
}

// TestSeedDefaultRespectsExistingProviders verifies the opt-out: a store
// that already configures any provider keeps it untouched. Shares the
// declaration registered by TestSeedDefaultSeedsDeclaredDefault; the
// assertions hold either way, so the test stays valid when run alone.
func TestSeedDefaultRespectsExistingProviders(t *testing.T) {
	store := config.NewStore()
	store.SetProvider(&config.ProviderSpec{
		ID:       "mine",
		Provider: "subscription",
		Config:   map[string]any{"url": "https://example.com/sub"},
	})
	if got := SeedDefault(store); got {
		t.Fatal("SeedDefault on a store with providers should be a no-op")
	}
	if _, ok := store.Provider("fake-default-seed-test"); ok {
		t.Fatal("default provider should not be seeded over an explicit provider list")
	}
	if _, ok := store.Provider("mine"); !ok {
		t.Fatal("explicitly configured provider should survive")
	}
}
