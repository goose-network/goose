// Package main is the goose engine entrypoint. It loads config (from a file
// or empty defaults), constructs the engine, starts the admin API, and waits
// for signals.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/goose-network/goose/include" // register built-in outbound plugins
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/engine"
	"github.com/goose-network/goose/internal/plugin"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to config JSON (optional)")
	flag.Parse()
	log.Printf("goose: registered outbound protocols: %v", plugin.Registered())

	store := config.NewStore()
	if cfgPath != "" {
		if err := loadConfig(store, cfgPath); err != nil {
			log.Fatalf("goose: load config %s: %v", cfgPath, err)
		}
	}

	eng, err := engine.New(store)
	if err != nil {
		log.Fatalf("goose: start engine: %v", err)
	}
	if err := eng.StartAPI(); err != nil {
		log.Fatalf("goose: start api: %v", err)
	}
	log.Printf("goose engine running; admin API on %s", store.Engine().API.Listen)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("goose: shutting down")
	_ = eng.Close()
}

// loadConfig loads a JSON config file describing the engine + inbounds +
// outbounds + pools + chains into the store.
func loadConfig(store *config.Store, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc configDoc
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
	return nil
}

// configDoc is the on-disk config shape.
type configDoc struct {
	Engine    config.Engine        `json:"engine"`
	Inbounds  []*config.Inbound    `json:"inbounds"`
	Outbounds []*config.OutboundSpec `json:"outbounds"`
	Pools     []*config.Pool       `json:"pools"`
	Chains    []*config.ChainSpec  `json:"chains"`
}
