// Package engine wires the engine's subsystems together: config store,
// metrics store, network stack, router, inbound listeners, and the admin
// API. It subscribes to config changes and reconciles its runtime listeners
// (starting/stopping inbound ports) when inbounds are added or removed.
package engine

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goose-network/goose/internal/api"
	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/geo"
	"github.com/goose-network/goose/internal/inbound"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/router"
	"github.com/goose-network/goose/internal/stack"
)

// listenerEntry pairs a running inbound listener with the signature of the
// config that produced it, so reconcile can detect credential/policy changes
// that don't change the listen address.
type listenerEntry struct {
	inbound.Listener
	sig string
}

// Engine is the running goose engine.
type Engine struct {
	cfg     *config.Store
	ms      *metrics.Store
	geo     *geo.Manager
	stack   stack.Stack
	router  *router.Router
	api     *api.Server

	mu        sync.Mutex
	listeners map[string]*listenerEntry // keyed by inbound id
	apiSrv    *http.Server

	// done is closed in Close to stop the watch goroutine and make reconcile
	// a no-op, so config changes arriving during shutdown cannot spawn new
	// listeners after Close has torn the existing ones down.
	done  chan struct{}
	once  sync.Once
	closed bool
}

// New constructs and starts the engine from a config store.
func New(cfg *config.Store) (*Engine, error) {
	engCfg := cfg.Engine()

	stk, err := stack.New(engCfg.Stack)
	if err != nil {
		return nil, fmt.Errorf("engine: stack: %w", err)
	}

	ms, err := metrics.Open(engCfg.DB)
	if err != nil {
		stk.Close()
		return nil, fmt.Errorf("engine: metrics: %w", err)
	}

	geoMgr := geo.NewManager(geo.Noop()) // TODO: wire real resolver from engCfg.Geo

	rtr := router.New(cfg, ms, stk)

	e := &Engine{
		cfg:       cfg,
		ms:        ms,
		geo:       geoMgr,
		stack:     stk,
		router:    rtr,
		listeners: map[string]*listenerEntry{},
		done:      make(chan struct{}),
	}
	e.api = api.New(cfg, ms)

	// initial reconcile of listeners
	e.reconcile()

	// subscribe to config changes for hot reload
	go e.watch(cfg.Subscribe())

	return e, nil
}

// watch reconciles listeners on every config version change. It returns when
// the engine's done channel is closed (in Close), so the goroutine does not
// leak past shutdown.
func (e *Engine) watch(ch <-chan uint64) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			e.router.Rebuild()
			e.reconcile()
		case <-e.done:
			return
		}
	}
}

// reconcile starts/stops inbound listeners to match the current config. It
// restarts a listener when any field that affects its behavior changes — not
// just the listen address, but also credentials and per-user policy — so
// hot-reloading users/credentials (requirement #4) actually takes effect.
// It is a no-op once the engine is closed.
func (e *Engine) reconcile() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	want := map[string]*config.Inbound{}
	for _, in := range e.cfg.Inbounds() {
		want[in.ID] = in
	}
	// stop removed
	for id, ln := range e.listeners {
		if _, ok := want[id]; !ok {
			_ = ln.Close()
			delete(e.listeners, id)
		}
	}
	// start new / restart changed. We restart when the protocol differs OR
	// the normalized listen address differs OR the credential/policy
	// signature differs. Address normalization is needed because the OS
	// resolves the listen address (e.g. ":8080" -> "[::]:8080") so a naive
	// string compare against the raw config would spuriously restart every
	// time.
	for id, in := range want {
		existing, ok := e.listeners[id]
		if ok && existing.Protocol() == in.Protocol &&
			normalizeListen(existing.Address()) == normalizeListen(in.Listen) &&
			inboundSignature(in) == existing.sig {
			continue // unchanged
		}
		if ok {
			_ = existing.Close()
			delete(e.listeners, id)
		}
		auth := inbound.NewAuthStore(toInboundUsers(in.Users))
		resolve := func(user string) string {
			// user-specific chain overrides inbound default
			for _, u := range in.Users {
				if u.Username == user && u.Policy != nil && u.Policy.ChainID != "" {
					return u.Policy.ChainID
				}
			}
			return in.Policy.ChainID
		}
		ln, err := inbound.New(in.ID, in.Protocol, in.Listen, auth, resolve, e.router)
		if err != nil {
			continue // log in real impl; keep engine running
		}
		e.listeners[id] = &listenerEntry{Listener: ln, sig: inboundSignature(in)}
	}
}

// normalizeListen canonicalizes a listen address so that config forms like
// ":8080", "0.0.0.0:8080" and the OS-resolved "[::]:8080" compare equal.
func normalizeListen(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = ""
	}
	return net.JoinHostPort(host, port)
}

// inboundSignature returns a string that changes when the inbound's
// credentials or per-user policy change, so reconcile can detect hot-reload
// of users without restarting on address-only noise.
func inboundSignature(in *config.Inbound) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", len(in.Users))
	for _, u := range in.Users {
		fmt.Fprintf(&b, "%s=%s:%s|", u.Username, u.Password, userChain(u.Policy))
	}
	fmt.Fprintf(&b, "chain=%s", in.Policy.ChainID)
	return b.String()
}

func userChain(p *config.InboundPolicy) string {
	if p == nil {
		return ""
	}
	return p.ChainID
}

func toInboundUsers(users []config.User) []inbound.User {
	out := make([]inbound.User, 0, len(users))
	for _, u := range users {
		out = append(out, inbound.User{Username: u.Username, Password: u.Password})
	}
	return out
}

// StartAPI starts the admin API server (non-blocking).
func (e *Engine) StartAPI() error {
	engCfg := e.cfg.Engine()
	h := e.api.Auth(engCfg.API.Token)
	e.apiSrv = &http.Server{
		Addr:              engCfg.API.Listen,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = e.apiSrv.ListenAndServe() }()
	return nil
}

// Close shuts the engine down gracefully. It signals the watch goroutine to
// stop and marks the engine closed so any config change racing in during
// shutdown cannot spawn new listeners.
func (e *Engine) Close() error {
	// Fully idempotent: the whole shutdown runs exactly once. A second Close
	// (e.g. an explicit call followed by a t.Cleanup, or a deferred Close
	// after an error) must be a no-op, not a panic — metrics.Close closes a
	// channel and stack.Close may close a TUN, neither of which tolerates a
	// double close.
	e.once.Do(func() {
		close(e.done)
		e.mu.Lock()
		e.closed = true
		for _, ln := range e.listeners {
			_ = ln.Close()
		}
		e.listeners = nil
		e.mu.Unlock()
		if e.apiSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = e.apiSrv.Shutdown(ctx)
		}
		_ = e.ms.Close()
		_ = e.stack.Close()
	})
	return nil
}
