// Package inbound implements the engine's ingress: HTTP and SOCKS5 proxy
// listeners with authentication. A single listener port can carry multiple
// users, each resolving to its own InboundPolicy (chain). Authenticated
// connections are handed to a Handler that routes them through the engine.
package inbound

import (
	"crypto/subtle"
	"sync"
)

// AuthStore is the per-listener authenticator. It maps username→password and
// is safe for concurrent use. A listener with an empty store accepts
// unauthenticated connections (no-auth).
type AuthStore struct {
	mu    sync.RWMutex
	users map[string]string // username -> password
}

// NewAuthStore builds an authenticator from a list of (user, pass) pairs.
// An empty list means no auth.
func NewAuthStore(users []User) *AuthStore {
	a := &AuthStore{users: map[string]string{}}
	for _, u := range users {
		a.users[u.Username] = u.Password
	}
	return a
}

// User is one inbound credential.
type User struct {
	Username string
	Password string
}

// Enabled reports whether authentication is required.
func (a *AuthStore) Enabled() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.users) > 0
}

// Verify returns true if the credentials match a known user, or if auth is
// disabled (in which case any credentials — including none — are accepted).
// The password comparison is constant-time (crypto/subtle) to avoid a timing
// side-channel that could leak the password byte-by-byte. The user-exists
// branch is also kept constant-time by running a dummy comparison when the
// user is unknown, so an attacker cannot distinguish "no such user" from
// "wrong password" by timing.
func (a *AuthStore) Verify(user, pass string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.users) == 0 {
		return true
	}
	want, ok := a.users[user]
	// Always run exactly one constant-time comparison over the full length of
	// the supplied password, against the real password for a known user or a
	// self-comparison for an unknown one. This keeps both the comparison
	// length and the result-independent control flow the same whether or not
	// the user exists, so timing cannot reveal user existence (only the
	// final ok && match differs, which is the intended accept/reject).
	if !ok {
		want = pass // compare pass==pass: full-length, returns 1, then ok&&match => false
	}
	match := subtle.ConstantTimeCompare([]byte(want), []byte(pass)) == 1
	return ok && match
}

// Users returns the list of configured usernames.
func (a *AuthStore) Users() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, 0, len(a.users))
	for u := range a.users {
		out = append(out, u)
	}
	return out
}
