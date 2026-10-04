// Package api implements the RESTful admin server. It exposes endpoints to
// create/list/delete inbounds, outbounds, pools, and chains, import proxies,
// and query metrics — all backed by the live config store. The server
// optionally authenticates with a bearer token.
//
// The OpenAPI 2.0 (Swagger) document for this API is generated from the swag
// annotations below by `make openapi` and committed at
// internal/api/docs/openapi.json. It feeds the TypeScript SDK generator.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/metrics"
)

//	@title			goose admin API
//	@version		1.0
//	@description	Admin API of the goose proxy-pool engine: create and manage inbounds (http/socks5 listeners), outbounds (proxy plugins), pools and chains, and inspect request metrics.
//
//	@tag.name		engine
//	@tag.description	Engine-level configuration (network stack, API settings, DB path)
//	@tag.name		inbounds
//	@tag.description	Inbound listeners clients connect to
//	@tag.name		outbounds
//	@tag.description	Outbound proxy specs the engine can dial through
//	@tag.name		pools
//	@tag.description	Named outbound sets with filters + selection strategy
//	@tag.name		chains
//	@tag.description	Ordered proxy chains of pool layers
//	@tag.name		providers
//	@tag.description	Dynamic outbound providers (subscription links, tunnel-based sources) that populate managed pools
//	@tag.name		metrics
//	@tag.description	Recent proxied-request records

// maxBodyBytes caps the size of an admin-API request body to prevent memory caps the size of an admin-API request body to prevent memory
// exhaustion from oversized POST/PUT payloads.
const maxBodyBytes = 1 << 20 // 1 MiB

// Server is the admin API server.
type Server struct {
	cfg *config.Store
	ms  *metrics.Store
	mux *http.ServeMux
}

// New builds an API server bound to the config store and metrics store.
func New(cfg *config.Store, ms *metrics.Store) *Server {
	s := &Server{cfg: cfg, ms: ms, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the HTTP handler (for embedding in a custom server).
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("/api/inbounds", s.inbounds)
	s.mux.HandleFunc("/api/inbounds/", s.inbounds)
	s.mux.HandleFunc("/api/outbounds", s.outbounds)
	s.mux.HandleFunc("/api/outbounds/", s.outbounds)
	s.mux.HandleFunc("/api/pools", s.pools)
	s.mux.HandleFunc("/api/pools/", s.pools)
	s.mux.HandleFunc("/api/chains", s.chains)
	s.mux.HandleFunc("/api/chains/", s.chains)
	s.mux.HandleFunc("/api/providers", s.providers)
	s.mux.HandleFunc("/api/providers/", s.providers)
	s.mux.HandleFunc("/api/metrics", s.metricsHandler)
	s.mux.HandleFunc("/api/engine", s.engine)
}

// Auth wraps the handler with bearer-token auth if a token is configured.
// The token comparison is constant-time to avoid a timing side-channel that
// could leak the admin token byte-by-byte.
func (s *Server) Auth(token string) http.Handler {
	if token == "" {
		return s.mux
	}
	tok := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		const prefix = "Bearer "
		// Constant-time prefix check: compare the first len(prefix) bytes,
		// then constant-time compare the remainder against the token.
		if len(h) <= len(prefix) ||
			subtle.ConstantTimeCompare([]byte(h[:len(prefix)]), []byte(prefix)) != 1 ||
			subtle.ConstantTimeCompare([]byte(h[len(prefix):]), tok) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	// Limit the body size to prevent memory-exhaustion DoS against the
	// admin API. http.MaxBytesReader also arranges for the server to close
	// the connection on overflow, stopping a slow-loris-style sender.
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("missing or invalid request body")
		}
		return err
	}
	return nil
}

// resourceID extracts the resource id from a request path like
// "/api/inbounds" (collection, id=="") or "/api/inbounds/foo" (item, id=="foo").
// It tolerates a trailing slash on the collection path, which the net/http
// ServeMux redirects to and which would otherwise be misrouted.
func resourceID(path, prefix string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, prefix), "/")
}

// errResponse is the error body shape used by every endpoint that rejects a
// request (400 bad JSON / validation, 401 unauthorized, 404 missing, 405
// wrong method). Declared once so the OpenAPI document can reference it.
type errResponse struct {
	// Error is a short, human-readable reason for the rejection.
	Error string `json:"error" example:"missing id"`
}

// --- engine ---

// engine godoc
//
//	@Summary      Get engine configuration
//	@Description  Returns the engine-level configuration: network stack selection, admin API settings, metrics DB path and offline geo database settings.
//	@Tags         engine
//	@Produce      json
//	@Success      200 {object} config.Engine
//	@Failure      401 {object} errResponse
//	@Router       /api/engine [get]
func (s *Server) engine(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.cfg.Engine(), http.StatusOK)
	case http.MethodPut, http.MethodPost:
		var e config.Engine
		if err := readJSON(r, &e); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if err := validateEngine(e); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		s.cfg.SetEngine(e)
		writeJSON(w, e, http.StatusOK)
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// updateEngine godoc
//
//	@Summary      Update engine configuration
//	@Description  Replaces the engine-level configuration (network stack, admin API settings, metrics DB path, geo database). The stack must be one of "system" or "gvisor" (or empty). Changes are applied live: the engine reconciles listeners on the next config version bump.
//	@Tags         engine
//	@Accept       json
//	@Produce      json
//	@Param        engine body config.Engine true "Engine configuration"
//	@Success      200 {object} config.Engine
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/engine [put]

// validateEngine sanity-checks an engine config before persisting it, so
// invalid values (e.g. an unknown stack type) are rejected at the API with a
// 400 rather than failing later during reconciliation.
func validateEngine(e config.Engine) error {
	switch e.Stack {
	case "", "system", "gvisor":
	default:
		return errors.New("engine: stack must be one of system, gvisor")
	}
	return nil
}

// --- inbounds ---

// listInbounds godoc
//
//	@Summary      List inbounds
//	@Description  Returns all configured inbound listeners.
//	@Tags         inbounds
//	@Produce      json
//	@Success      200 {array} config.Inbound
//	@Failure      401 {object} errResponse
//	@Router       /api/inbounds [get]
//
// getInbound godoc
//
//	@Summary      Get an inbound
//	@Description  Returns the inbound listener with the given id.
//	@Tags         inbounds
//	@Produce      json
//	@Param        id path string true "Inbound id"
//	@Success      200 {object} config.Inbound
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/inbounds/{id} [get]
//
// createInbound godoc
//
//	@Summary      Create or replace an inbound
//	@Description  Creates (or replaces) an inbound listener. When the body carries no id, the id must be supplied in the URL path (/api/inbounds/{id}); when both are present they must match. The policy routes the inbound (or, via per-user policy overrides, each user) to either a named chain (chain_id), a single pool (pool_id, optionally narrowed by filters), or — when neither is set — any available outbound. Changes apply live: the engine starts/stops listeners on the next config version bump.
//	@Tags         inbounds
//	@Accept       json
//	@Produce      json
//	@Param        id path string false "Inbound id (ignored unless the body omits it)"
//	@Param        inbound body config.Inbound true "Inbound to create"
//	@Success      201 {object} config.Inbound
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/inbounds [post]
//
// deleteInbound godoc
//
//	@Summary      Delete an inbound
//	@Description  Removes the inbound listener with the given id and stops it.
//	@Tags         inbounds
//	@Param        id path string true "Inbound id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/inbounds/{id} [delete]
func (s *Server) inbounds(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/inbounds")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Inbounds(), http.StatusOK)
	case r.Method == http.MethodGet:
		if in, ok := s.cfg.Inbound(id); ok {
			writeJSON(w, in, http.StatusOK)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var in config.Inbound
		if err := readJSON(r, &in); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if id != "" {
			// Item path: the URL identifies the resource; the body id (if
			// any) must match. This prevents POST /api/inbounds/foo from
			// silently creating/updating a different resource (/bar).
			if in.ID != "" && in.ID != id {
				writeJSON(w, errResponse{"id in body does not match URL"}, http.StatusBadRequest)
				return
			}
			in.ID = id
		}
		if in.ID == "" {
			writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
			return
		}
		s.cfg.SetInbound(&in)
		writeJSON(w, in, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteInbound(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// --- outbounds ---

// listOutbounds godoc
//
//	@Summary      List outbounds
//	@Description  Returns all configured outbound proxy specs (static and provider-managed).
//	@Tags         outbounds
//	@Produce      json
//	@Success      200 {array} config.OutboundSpec
//	@Failure      401 {object} errResponse
//	@Router       /api/outbounds [get]
//
// getOutbound godoc
//
//	@Summary      Get an outbound
//	@Description  Returns the outbound spec with the given id.
//	@Tags         outbounds
//	@Produce      json
//	@Param        id path string true "Outbound id"
//	@Success      200 {object} config.OutboundSpec
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/outbounds/{id} [get]
//
// createOutbound godoc
//
//	@Summary      Create or replace an outbound
//	@Description  Creates (or replaces) an outbound spec. The protocol names a registered outbound plugin (e.g. "direct", "http", "socks5", "psiphon"); config is the plugin-specific configuration map.
//	@Tags         outbounds
//	@Accept       json
//	@Produce      json
//	@Param        id path string false "Outbound id (ignored unless the body omits it)"
//	@Param        outbound body config.OutboundSpec true "Outbound to create"
//	@Success      201 {object} config.OutboundSpec
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/outbounds [post]
//
// deleteOutbound godoc
//
//	@Summary      Delete an outbound
//	@Description  Removes the outbound with the given id. Provider-managed outbounds are re-added by their provider on its next refresh.
//	@Tags         outbounds
//	@Param        id path string true "Outbound id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/outbounds/{id} [delete]
func (s *Server) outbounds(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/outbounds")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Outbounds(), http.StatusOK)
	case r.Method == http.MethodGet:
		if o, ok := s.cfg.Outbound(id); ok {
			writeJSON(w, o, http.StatusOK)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var o config.OutboundSpec
		if err := readJSON(r, &o); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if id != "" {
			if o.ID != "" && o.ID != id {
				writeJSON(w, errResponse{"id in body does not match URL"}, http.StatusBadRequest)
				return
			}
			o.ID = id
		}
		if o.ID == "" {
			writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
			return
		}
		s.cfg.SetOutbound(&o)
		writeJSON(w, o, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteOutbound(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// --- pools ---

// listPools godoc
//
//	@Summary      List pools
//	@Description  Returns all configured outbound pools (ordered outbound sets with filters + selection strategy).
//	@Tags         pools
//	@Produce      json
//	@Success      200 {array} config.Pool
//	@Failure      401 {object} errResponse
//	@Router       /api/pools [get]
//
// getPool godoc
//
//	@Summary      Get a pool
//	@Description  Returns the pool with the given id.
//	@Tags         pools
//	@Produce      json
//	@Param        id path string true "Pool id"
//	@Success      200 {object} config.Pool
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/pools/{id} [get]
//
// createPool godoc
//
//	@Summary      Create or replace a pool
//	@Description  Creates (or replaces) a pool: an ordered set of outbound ids plus the filters and selection strategy applied when picking from it.
//	@Tags         pools
//	@Accept       json
//	@Produce      json
//	@Param        id path string false "Pool id (ignored unless the body omits it)"
//	@Param        pool body config.Pool true "Pool to create"
//	@Success      201 {object} config.Pool
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/pools [post]
//
// deletePool godoc
//
//	@Summary      Delete a pool
//	@Description  Removes the pool with the given id.
//	@Tags         pools
//	@Param        id path string true "Pool id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/pools/{id} [delete]
func (s *Server) pools(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/pools")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Pools(), http.StatusOK)
	case r.Method == http.MethodGet:
		if p, ok := s.cfg.Pool(id); ok {
			writeJSON(w, p, http.StatusOK)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var p config.Pool
		if err := readJSON(r, &p); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if id != "" {
			if p.ID != "" && p.ID != id {
				writeJSON(w, errResponse{"id in body does not match URL"}, http.StatusBadRequest)
				return
			}
			p.ID = id
		}
		if p.ID == "" {
			writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
			return
		}
		s.cfg.SetPool(&p)
		writeJSON(w, p, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeletePool(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// --- chains ---

// listChains godoc
//
//	@Summary      List chains
//	@Description  Returns all configured proxy chains (ordered lists of pool layers).
//	@Tags         chains
//	@Produce      json
//	@Success      200 {array} config.ChainSpec
//	@Failure      401 {object} errResponse
//	@Router       /api/chains [get]
//
// getChain godoc
//
//	@Summary      Get a chain
//	@Description  Returns the chain with the given id.
//	@Tags         chains
//	@Produce      json
//	@Param        id path string true "Chain id"
//	@Success      200 {object} config.ChainSpec
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/chains/{id} [get]
//
// createChain godoc
//
//	@Summary      Create or replace a chain
//	@Description  Creates (or replaces) a proxy chain: an ordered list of layers, each referencing a pool (which carries its own filters + selector). Layer 0 is the first hop; each subsequent layer is dialed through the previous.
//	@Tags         chains
//	@Accept       json
//	@Produce      json
//	@Param        id path string false "Chain id (ignored unless the body omits it)"
//	@Param        chain body config.ChainSpec true "Chain to create"
//	@Success      201 {object} config.ChainSpec
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/chains [post]
//
// deleteChain godoc
//
//	@Summary      Delete a chain
//	@Description  Removes the chain with the given id. Inbounds whose policy still references it will fail to resolve until reconfigured.
//	@Tags         chains
//	@Param        id path string true "Chain id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/chains/{id} [delete]
func (s *Server) chains(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/chains")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Chains(), http.StatusOK)
	case r.Method == http.MethodGet:
		if c, ok := s.cfg.Chain(id); ok {
			writeJSON(w, c, http.StatusOK)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var c config.ChainSpec
		if err := readJSON(r, &c); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if id != "" {
			if c.ID != "" && c.ID != id {
				writeJSON(w, errResponse{"id in body does not match URL"}, http.StatusBadRequest)
				return
			}
			c.ID = id
		}
		if c.ID == "" {
			writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
			return
		}
		s.cfg.SetChain(&c)
		writeJSON(w, c, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteChain(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// --- metrics ---

// listMetrics godoc
//
//	@Summary      List recent request metrics
//	@Description  Returns up to n recent proxied-request records, newest first, each carrying the chain that served it. n defaults to 100; values above 10000 are capped; invalid values fall back to the default.
//	@Tags         metrics
//	@Produce      json
//	@Param        n query int false "Number of records to return (default 100, max 10000)" default(100) maximum(10000)
//	@Success      200 {array} core.RequestMetric
//	@Failure      401 {object} errResponse
//	@Failure      500 {object} errResponse
//	@Router       /api/metrics [get]
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
		return
	}
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := parseInt(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	// Cap n to a sane maximum so a huge ?n= cannot make Recent allocate a
	// slice that exhausts memory (and so a wrapped-to-huge value from a
	// very large but valid input is bounded).
	if n > maxMetricsRows {
		n = maxMetricsRows
	}
	rec, err := s.ms.Recent(n)
	if err != nil {
		writeJSON(w, errResponse{err.Error()}, http.StatusInternalServerError)
		return
	}
	writeJSON(w, rec, http.StatusOK)
}

// maxMetricsRows caps the number of metric rows a single API call can return.
const maxMetricsRows = 10000

func parseInt(s string) (int, error) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errBadInt
		}
		// Guard against integer overflow: stop accumulating once we would
		// exceed the positive int range, so a multi-gigabyte ?n= value
		// cannot wrap to a huge positive int and crash the server with
		// "makeslice: cap out of range".
		if n > (maxInt-int(c-'0'))/10 {
			return 0, errBadInt
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

const maxInt = int(^uint(0) >> 1)

var errBadInt = &errString{"bad integer"}

type errString struct{ s string }

func (e *errString) Error() string { return e.s }

// --- providers ---

// listProviders godoc
//
//	@Summary      List providers
//	@Description  Returns all configured dynamic outbound providers. Each provider owns a managed pool that its outbounds are merged into; providers created without a pool_id report the default pool "default".
//	@Tags         providers
//	@Produce      json
//	@Success      200 {array} config.ProviderSpec
//	@Failure      401 {object} errResponse
//	@Router       /api/providers [get]
//
// getProvider godoc
//
//	@Summary      Get a provider
//	@Description  Returns the provider spec with the given id.
//	@Tags         providers
//	@Produce      json
//	@Param        id path string true "Provider id"
//	@Success      200 {object} config.ProviderSpec
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/providers/{id} [get]
//
// createProvider godoc
//
//	@Summary      Create or replace a provider
//	@Description  Creates (or replaces) a dynamic outbound provider. The provider names a registered provider plugin (e.g. "psiphon", "subscription", "mihomo", "singbox"); pool_id is the managed pool its outbounds are merged into, defaulting to the "default" pool when omitted; config is plugin-specific. Changes apply live: the engine starts, restarts, or stops the provider's poll loop on the next config version bump.
//	@Tags         providers
//	@Accept       json
//	@Produce      json
//	@Param        id path string false "Provider id (ignored unless the body omits it)"
//	@Param        provider body config.ProviderSpec true "Provider to create"
//	@Success      201 {object} config.ProviderSpec
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Router       /api/providers [post]
//
// deleteProvider godoc
//
//	@Summary      Delete a provider
//	@Description  Removes the provider with the given id, stops its poll loop, and drops its managed outbounds and pool.
//	@Tags         providers
//	@Param        id path string true "Provider id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/providers/{id} [delete]
func (s *Server) providers(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/providers")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Providers(), http.StatusOK)
	case r.Method == http.MethodGet:
		if p, ok := s.cfg.Provider(id); ok {
			writeJSON(w, p, http.StatusOK)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var p config.ProviderSpec
		if err := readJSON(r, &p); err != nil {
			writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
			return
		}
		if id != "" {
			if p.ID != "" && p.ID != id {
				writeJSON(w, errResponse{"id in body does not match URL"}, http.StatusBadRequest)
				return
			}
			p.ID = id
		}
		if p.ID == "" {
			writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
			return
		}
		// An omitted pool_id means the default pool, so a provider can be
		// created with just a plugin + config (the common case: paste a
		// subscription URL, land in "default").
		if p.PoolID == "" {
			p.PoolID = config.DefaultPoolID
		}
		s.cfg.SetProvider(&p)
		writeJSON(w, p, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteProvider(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}
