package provider

import (
	"testing"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// The registry is process-wide and accumulates declarations across tests, so
// these tests share a fixed set of fakes and assert on SPECIFIC ids rather
// than exact seed counts — that keeps them valid under -shuffle and run alone.
// fakeA always carries the same Config so first-wins registration cannot
// change what the Config-propagation test observes.
var (
	fakeA = pub.DefaultProvider{
		ID:       "fake-default-seed-test",
		Provider: "fake-default-seed-test",
		Config:   map[string]any{"some": "default"},
	}
	fakeB = pub.DefaultProvider{ID: "fake-default-seed-test-b", Provider: "fake-default-seed-test-b"}
)

// ensureDefault registers a declaration unless one for that provider already
// exists (RegisterDefaultProvider panics on a duplicate provider name).
func ensureDefault(dp pub.DefaultProvider) {
	for _, d := range pub.LookupDefaultProviders() {
		if d.Provider == dp.Provider {
			return
		}
	}
	pub.RegisterDefaultProvider(dp)
}

// TestSeedDefaultSeedsDeclaredDefault verifies the plugin-agnostic seeding:
// an empty store gains the provider a plugin declared through the registry,
// and a second call is a no-op (every declaration is now configured). Which
// plugin declared itself is invisible to the engine.
func TestSeedDefaultSeedsDeclaredDefault(t *testing.T) {
	ensureDefault(fakeA)

	store := config.NewStore()
	if n := SeedDefault(store); n == 0 {
		t.Fatal("SeedDefault on an empty store should seed the declared default")
	}
	spec, ok := store.Provider(fakeA.ID)
	if !ok {
		t.Fatal("seeded default provider not in store")
	}
	if spec.Provider != fakeA.Provider {
		t.Fatalf("spec.Provider = %q, want the declared provider name", spec.Provider)
	}
	if v, _ := spec.Config["some"].(string); v != "default" {
		t.Fatalf("spec.Config propagated incorrectly: %v", spec.Config)
	}
	// EffectivePoolIDs defaults an unset pool list to the default pool.
	if got := spec.EffectivePoolIDs(); len(got) != 1 || got[0] != config.DefaultPoolID {
		t.Fatalf("seeded spec pools = %v, want [%q]", got, config.DefaultPoolID)
	}

	// Idempotent: the first call seeded every declaration, so a second call
	// against the now-populated store adds nothing.
	if n := SeedDefault(store); n != 0 {
		t.Fatalf("second SeedDefault = %d, want 0 (all declarations configured)", n)
	}
}

// TestSeedDefaultAddsAlongsideExistingProviders verifies the new policy: a
// store that already configures a DIFFERENT plugin still gains the declared
// zero-config default — auto-seeding is per-plugin, not fresh-only.
func TestSeedDefaultAddsAlongsideExistingProviders(t *testing.T) {
	ensureDefault(fakeA)

	store := config.NewStore()
	store.SetProvider(&config.ProviderSpec{
		ID:       "mine",
		Provider: "subscription",
		Config:   map[string]any{"url": "https://example.com/sub"},
	})
	SeedDefault(store)
	if _, ok := store.Provider(fakeA.ID); !ok {
		t.Fatal("declared zero-config default should be seeded alongside an explicit provider")
	}
	if _, ok := store.Provider("mine"); !ok {
		t.Fatal("explicitly configured provider should survive")
	}
}

// TestSeedDefaultSkipsAlreadyConfiguredPlugin verifies the per-plugin opt-out:
// a store that already configures a provider using the declared plugin keeps
// it untouched and is NOT given a second instance of that plugin. (Other
// declarations may still seed; the assertion is about this plugin only.)
func TestSeedDefaultSkipsAlreadyConfiguredPlugin(t *testing.T) {
	ensureDefault(fakeA)

	store := config.NewStore()
	store.SetProvider(&config.ProviderSpec{
		ID:       "custom-A",
		Provider: fakeA.Provider,
		Config:   map[string]any{"explicit": true},
	})
	SeedDefault(store)
	if _, ok := store.Provider(fakeA.ID); ok {
		t.Fatal("a second instance of an already-configured plugin must not be seeded")
	}
	if spec, ok := store.Provider("custom-A"); !ok || spec.Config["explicit"] != true {
		t.Fatal("the explicit spec for the plugin should survive untouched")
	}
}

// TestSeedDefaultSeedsEveryDeclaration verifies multiplicity: every linked
// zero-config plugin is seeded, not just one.
func TestSeedDefaultSeedsEveryDeclaration(t *testing.T) {
	ensureDefault(fakeA)
	ensureDefault(fakeB)

	store := config.NewStore()
	SeedDefault(store)
	for _, dp := range []pub.DefaultProvider{fakeA, fakeB} {
		if _, ok := store.Provider(dp.ID); !ok {
			t.Fatalf("declared default %q not seeded", dp.ID)
		}
	}
}

// TestSeedDefaultRespectsDisableEnv verifies the operator opt-out:
// GOOSE_NO_DEFAULT_PROVIDER=1 leaves the store as-is.
func TestSeedDefaultRespectsDisableEnv(t *testing.T) {
	ensureDefault(fakeA)
	t.Setenv("GOOSE_NO_DEFAULT_PROVIDER", "1")

	store := config.NewStore()
	if n := SeedDefault(store); n != 0 {
		t.Fatalf("SeedDefault = %d, want 0 with GOOSE_NO_DEFAULT_PROVIDER=1", n)
	}
	if _, ok := store.Provider(fakeA.ID); ok {
		t.Fatal("seeding must be disabled by GOOSE_NO_DEFAULT_PROVIDER=1")
	}
}