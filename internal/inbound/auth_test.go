package inbound

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthStoreVerify(t *testing.T) {
	a := NewAuthStore([]User{{Username: "alice", Password: "secret"}, {Username: "bob", Password: "pw"}})
	assert.True(t, a.Enabled())
	assert.True(t, a.Verify("alice", "secret"))
	assert.True(t, a.Verify("bob", "pw"))
	assert.False(t, a.Verify("alice", "wrong"))
	assert.False(t, a.Verify("unknown", "secret"))
}

func TestAuthStoreDisabled(t *testing.T) {
	a := NewAuthStore(nil)
	assert.False(t, a.Enabled())
	// when disabled, any credentials (including none) are accepted
	assert.True(t, a.Verify("", ""))
	assert.True(t, a.Verify("anyone", "anything"))
}

func TestAuthStoreUsers(t *testing.T) {
	a := NewAuthStore([]User{{Username: "alice", Password: "s"}, {Username: "bob", Password: "p"}})
	users := a.Users()
	assert.ElementsMatch(t, []string{"alice", "bob"}, users)
}
