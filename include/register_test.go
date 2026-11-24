package include

import (
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/provider"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// TestZeroConfigDefaultsSeeded guards the distro's plugin set: every built-in
// zero-config provider plugin blank-imported here declares an auto-seeded
// default, and a fresh config store gains them all through provider.SeedDefault
// — so the engine retrieves their proxy servers and shows them in Live provider
// status with no goose.json setup. It catches a plugin silently dropping its
// RegisterDefaultProvider declaration (which would make it vanish from the
// auto-configured set) and exercises the real entrypoint seeding path against
// the linked plugins rather than a fake registry.
func TestZeroConfigDefaultsSeeded(t *testing.T) {
	t.Setenv("GOOSE_NO_DEFAULT_PROVIDER", "0")

	// The zero-config plugins this distro links. mihomo/singbox and the
	// built-in direct/http/socks5/subscription outbounds are not zero-config
	// providers and declare no default.
	want := []string{"geph", "openrung", "psiphon", "tor"}

	declared := map[string]bool{}
	for _, dp := range pub.LookupDefaultProviders() {
		declared[dp.Provider] = true
	}
	for _, name := range want {
		if !declared[name] {
			t.Errorf("zero-config plugin %q did not declare a default (RegisterDefaultProvider)", name)
		}
	}

	store := config.NewStore()
	if n := provider.SeedDefault(store); n < len(want) {
		t.Fatalf("SeedDefault seeded %d providers, want at least %d (%v)", n, len(want), want)
	}
	// Each declaration's ID defaults to its provider name, so a fresh store
	// holds a provider under each plugin name.
	for _, name := range want {
		if _, ok := store.Provider(name); !ok {
			t.Errorf("auto-seeded provider %q missing from a fresh store", name)
		}
	}
}