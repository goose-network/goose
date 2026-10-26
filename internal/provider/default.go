// Fresh-deployment default-provider seeding: the engine-side counterpart to a
// plugin's RegisterDefaultProvider declaration (see goose-plugin-api).
//
// A zero-config plugin (one that runs on its embedded defaults with an empty
// config map) can declare itself the default for a fresh deployment — a store
// with no providers at all, which is what a no-config engine start (the
// docker image's default) produces. SeedDefault converts the declaration into
// a ProviderSpec and seeds it, so the engine is routable — and its admin UI
// shows a provider — out of the box.
//
// The engine stays plugin-agnostic end to end: SeedDefault only asks the
// registry for the declaration (pub.LookupDefaultProvider); which plugin — if
// any — declared itself the default is entirely up to the plugin set linked
// into the build. A binary without a default-declaring plugin, a store that
// already configures any provider (an explicit provider list opts out), or
// the operator's GOOSE_NO_DEFAULT_PROVIDER=1 all leave the store as-is.
//
// Seeding is an entrypoint concern (cmd/goose), not engine.New: libraries and
// tests construct engines directly and must not suddenly gain live tunnels.
package provider

import (
	"os"
	"strconv"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// SeedDefault seeds the plugin-declared default provider (pub.LookupDefault-
// Provider) into store when it configures no providers. It is intended for
// the binary entrypoint after loading config, before engine.New, so the
// engine's initial reconcile and the provider manager both see the seeded
// spec. Returns whether a provider was added.
func SeedDefault(store *config.Store) bool {
	if v, ok := os.LookupEnv("GOOSE_NO_DEFAULT_PROVIDER"); ok {
		if b, err := strconv.ParseBool(v); err == nil && b {
			return false
		}
	}
	dp, ok := pub.LookupDefaultProvider()
	if !ok {
		return false
	}
	spec := &config.ProviderSpec{
		ID:       dp.ID,
		Provider: dp.Provider,
		Config:   dp.Config,
	}
	return store.SeedDefaultProvider(spec)
}
