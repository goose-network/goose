// Package main is the goose engine entrypoint. It loads config (from a file
// or empty defaults), constructs the engine, starts the admin API, and waits
// for signals.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/goose-network/goose/include" // register built-in outbound plugins
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/engine"
	"github.com/goose-network/goose/internal/plugin"
	"github.com/goose-network/goose/internal/provider"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to config JSON (optional)")
	flag.Parse()
	log.Printf("goose: registered outbound protocols: %v", plugin.Registered())

	// The engine's config is durable: every edit made through the admin API
	// is written back to this file and reloaded on the next start. An
	// explicit -config wins; otherwise GOOSE_PERSIST_PATH, else goose.json
	// beside the default goose.db. A missing explicit -config is fatal; the
	// implicit default simply starts fresh and is created on the first write.
	explicitConfig := cfgPath != ""
	persistPath := cfgPath
	if persistPath == "" {
		persistPath = os.Getenv("GOOSE_PERSIST_PATH")
		if persistPath == "" {
			persistPath = "goose.json"
		}
	}

	store := config.NewStore()
	if err := loadConfig(store, persistPath); err != nil {
		if explicitConfig || !errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("goose: load config %s: %v", persistPath, err)
		}
	}
	// Bound the engine's own log storage by default: an unset log.file
	// resolves to logs/goose.log beside the persisted config (the /data
	// volume in containers), so the shipped engine keeps a durable log
	// capped at 1 GB without any configuration. Resolving before the
	// persister's first write means the default survives restarts; set
	// engine.log.file to "-" to opt out explicitly.
	engCfg := store.Engine()
	if engCfg.Log.File == "" {
		engCfg.Log.File = config.DefaultLogPath(persistPath)
		store.SetEngine(engCfg)
	}
	// A hand-edited config bypasses the API's validation, so re-check the
	// log path here: aimed at the persisted config or the metrics DB it
	// would append log records into durable state and rotate/rename it out
	// from under the persister or bbolt. Refuse and stay stderr-only rather
	// than corrupt state or take the engine down.
	if f := engCfg.Log.File; f != "" && f != "-" {
		collision := config.SamePath(f, persistPath) ||
			config.SamePath(f, engCfg.DB) || config.SamePath(f, engCfg.DB+"-wal") || config.SamePath(f, engCfg.DB+"-shm")
		if collision {
			log.Printf("goose: log.file %s collides with durable state; file logging disabled", f)
			engCfg.Log.File = "-"
			store.SetEngine(engCfg)
		}
	}
	// Record the binary-known protected locations so PUT /api/engine can
	// reject log.file collisions with the LIVE metrics DB (never reopened on
	// config edits) and the persisted config, not just the incoming fields.
	store.SetRuntimePaths(config.RuntimePaths{PersistPath: persistPath, LiveDB: store.Engine().DB})
	// Auto-seed every zero-config provider plugin linked into the build that
	// is not already configured, so the engine is routable — and its admin UI
	// shows the providers — out of the box without editing goose.json. Which
	// plugins seed lives entirely in the plugin set; goose stays generic
	// (GOOSE_NO_DEFAULT_PROVIDER=1 disables seeding).
	if n := provider.SeedDefault(store); n > 0 {
		log.Printf("goose: seeded %d zero-config default provider(s) (set GOOSE_NO_DEFAULT_PROVIDER=1 to disable)", n)
	}

	// Write through: persist API edits (and the startup state above) to
	// persistPath. Failures only log — an unwritable path must not take the
	// engine down — and Stop performs a final flush so a shutdown right
	// after an edit is not lost to the debounce window.
	persister := config.NewPersister(store, persistPath)
	go persister.Run(2 * time.Second)

	eng, err := engine.New(store)
	if err != nil {
		log.Fatalf("goose: start engine: %v", err)
	}
	if err := eng.StartAPI(); err != nil {
		log.Fatalf("goose: start api: %v", err)
	}
	log.Printf("goose engine running; admin API on %s; config persisted to %s", store.Engine().API.Listen, persistPath)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("goose: shutting down")
	_ = eng.Close()
	persister.Stop()
}

// loadConfig loads a JSON config file describing the engine + inbounds +
// outbounds + pools + chains into the store.
func loadConfig(store *config.Store, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc config.Doc
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	store.SetEngine(doc.Engine)
	for _, in := range doc.Inbounds {
		store.SetInbound(in)
	}
	for _, o := range doc.Outbounds {
		store.SetOutbound(o)
	}
	for _, p := range doc.Pools {
		store.SetPool(p)
	}
	for _, c := range doc.Chains {
		store.SetChain(c)
	}
	for _, p := range doc.Providers {
		store.SetProvider(p)
	}
	return nil
}
