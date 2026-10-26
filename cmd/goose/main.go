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
	// A fresh deployment (no providers in the loaded config — the docker
	// image's no-config start) seeds the provider a plugin declared through
	// the registry, so the engine is routable out of the box. Which plugin
	// that is lives entirely in the plugin set; goose itself stays generic
	// (GOOSE_NO_DEFAULT_PROVIDER=1 disables seeding).
	if provider.SeedDefault(store) {
		log.Printf("goose: no providers configured; seeded the plugin-declared default provider (set GOOSE_NO_DEFAULT_PROVIDER=1 to disable)")
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
