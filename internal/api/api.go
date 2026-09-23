// Package api implements the RESTful admin server. It exposes endpoints to
// create/list/delete inbounds, outbounds, pools, and chains, import proxies,
// and query metrics — all backed by the live config store. The server
// optionally authenticates with a bearer token.
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

// maxBodyBytes caps the size of an admin-API request body to prevent memory
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

// --- engine ---

func (s *Server) engine(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.cfg.Engine(), http.StatusOK)
	case http.MethodPut, http.MethodPost:
		var e config.Engine
		if err := readJSON(r, &e); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := validateEngine(e); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.cfg.SetEngine(e)
		writeJSON(w, e, http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

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

func (s *Server) inbounds(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/inbounds")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Inbounds(), http.StatusOK)
	case r.Method == http.MethodGet:
		if in, ok := s.cfg.Inbound(id); ok {
			writeJSON(w, in, http.StatusOK)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var in config.Inbound
		if err := readJSON(r, &in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if id != "" {
			// Item path: the URL identifies the resource; the body id (if
			// any) must match. This prevents POST /api/inbounds/foo from
			// silently creating/updating a different resource (/bar).
			if in.ID != "" && in.ID != id {
				http.Error(w, "id in body does not match URL", http.StatusBadRequest)
				return
			}
			in.ID = id
		}
		if in.ID == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		s.cfg.SetInbound(&in)
		writeJSON(w, in, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteInbound(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- outbounds ---

func (s *Server) outbounds(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/outbounds")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Outbounds(), http.StatusOK)
	case r.Method == http.MethodGet:
		if o, ok := s.cfg.Outbound(id); ok {
			writeJSON(w, o, http.StatusOK)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var o config.OutboundSpec
		if err := readJSON(r, &o); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if id != "" {
			if o.ID != "" && o.ID != id {
				http.Error(w, "id in body does not match URL", http.StatusBadRequest)
				return
			}
			o.ID = id
		}
		if o.ID == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		s.cfg.SetOutbound(&o)
		writeJSON(w, o, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteOutbound(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- pools ---

func (s *Server) pools(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/pools")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Pools(), http.StatusOK)
	case r.Method == http.MethodGet:
		if p, ok := s.cfg.Pool(id); ok {
			writeJSON(w, p, http.StatusOK)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var p config.Pool
		if err := readJSON(r, &p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if id != "" {
			if p.ID != "" && p.ID != id {
				http.Error(w, "id in body does not match URL", http.StatusBadRequest)
				return
			}
			p.ID = id
		}
		if p.ID == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		s.cfg.SetPool(&p)
		writeJSON(w, p, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeletePool(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- chains ---

func (s *Server) chains(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/chains")
	switch {
	case r.Method == http.MethodGet && id == "":
		writeJSON(w, s.cfg.Chains(), http.StatusOK)
	case r.Method == http.MethodGet:
		if c, ok := s.cfg.Chain(id); ok {
			writeJSON(w, c, http.StatusOK)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var c config.ChainSpec
		if err := readJSON(r, &c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if id != "" {
			if c.ID != "" && c.ID != id {
				http.Error(w, "id in body does not match URL", http.StatusBadRequest)
				return
			}
			c.ID = id
		}
		if c.ID == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		s.cfg.SetChain(&c)
		writeJSON(w, c, http.StatusCreated)
	case r.Method == http.MethodDelete && id != "":
		if s.cfg.DeleteChain(id) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- metrics ---

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
