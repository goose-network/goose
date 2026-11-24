// Zero-config default-provider seeding: the engine-side counterpart to a
// plugin's RegisterDefaultProvider declaration (see goose-plugin-api).
//
// A zero-config plugin (one that runs on its embedded defaults with an empty
// config map) declares itself an auto-seeded default. SeedDefault converts
// every such declaration whose plugin is not already configured into a
// ProviderSpec and seeds it, so the engine is routable — and its admin UI
// shows the providers — out of the box, without the operator editing
// goose.json. A fresh deployment gains every linked zero-config plugin; a
// deployment that already configures some providers still gains the
// newly-linked ones (a plugin already configured opts out of its own seed).
//
// The engine stays plugin-agnostic end to end: SeedDefault only asks the
// registry for the declarations (pub.LookupDefaultProviders); which plugins
// declared themselves is entirely up to the plugin set linked into the build.
// A binary without a default-declaring plugin, or the operator's
// GOOSE_NO_DEFAULT_PROVIDER=1, leaves the store as-is.
//
// Seeding is an entrypoint concern (cmd/goose), not engine.New: libraries and
// tests construct engines directly and must not suddenly gain live tunnels.
package provider

import (
	"log"
	"os"
	"strconv"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// SeedDefault seeds every plugin-declared zero-config default (pub.Lookup-
// DefaultProviders) whose plugin is not already configured in store. It is
// intended for the binary entrypoint after loading config, before engine.New,
// so the engine's initial reconcile and the provider manager both see the
// seeded specs. A declaration whose plugin name already appears among the
// configured providers is skipped — an explicit spec for that plugin opts out
// of its seed. Returns the number of providers added.
func SeedDefault(store *config.Store) int {
	if v, ok := os.LookupEnv("GOOSE_NO_DEFAULT_PROVIDER"); ok {
		if b, err := strconv.ParseBool(v); err == nil && b {
			return 0
		}
	}
	dps := pub.LookupDefaultProviders()
	if len(dps) == 0 {
		return 0
	}
	configured := make(map[string]bool)
	for _, spec := range store.Providers() {
		configured[spec.Provider] = true
	}
	n := 0
	for _, dp := range dps {
		if configured[dp.Provider] {
			continue
		}
		spec := &config.ProviderSpec{
			ID:       dp.ID,
			Provider: dp.Provider,
			Config:   dp.Config,
		}
		if store.SeedDefaultProvider(spec) {
			n++
			configured[dp.Provider] = true
		} else {
			// The plugin is not configured, but its default seed ID is already
			// taken by another provider — surface it instead of silently
			// dropping the plugin from the Live status view.
			log.Printf("goose: auto-seed of %q skipped: provider id %q already configured (rename it to let %q seed)", dp.Provider, dp.ID, dp.Provider)
		}
	}
	return n
}