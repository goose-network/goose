package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreCRUD(t *testing.T) {
	s := NewStore()

	// inbounds
	s.SetInbound(&Inbound{ID: "in1", Protocol: "socks5", Listen: "127.0.0.1:1080"})
	in, ok := s.Inbound("in1")
	require.True(t, ok)
	assert.Equal(t, "socks5", in.Protocol)
	assert.Len(t, s.Inbounds(), 1)
	assert.True(t, s.DeleteInbound("in1"))
	assert.False(t, s.DeleteInbound("in1"))

	// outbounds
	s.SetOutbound(&OutboundSpec{ID: "ob1", Protocol: "http", Config: map[string]any{"address": "1.2.3.4:8080"}})
	ob, ok := s.Outbound("ob1")
	require.True(t, ok)
	assert.Equal(t, "http", ob.Protocol)

	// pools
	s.SetPool(&Pool{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: SelectorSpec{Type: "roundrobin"}})
	p, ok := s.Pool("p1")
	require.True(t, ok)
	assert.Equal(t, "roundrobin", p.Selector.Type)

	// chains
	s.SetChain(&ChainSpec{ID: "c1", Layers: []string{"p1"}})
	c, ok := s.Chain("c1")
	require.True(t, ok)
	assert.Equal(t, []string{"p1"}, c.Layers)
}

func TestStoreVersionBumps(t *testing.T) {
	s := NewStore()
	v0 := s.Version()
	s.SetInbound(&Inbound{ID: "in1"})
	assert.Greater(t, s.Version(), v0)
}

func TestStoreSubscribe(t *testing.T) {
	s := NewStore()
	ch := s.Subscribe()
	s.SetOutbound(&OutboundSpec{ID: "ob1"})
	select {
	case v := <-ch:
		assert.Greater(t, v, uint64(0))
	default:
		t.Fatal("subscriber did not receive version")
	}
}

func TestStoreEngineDefaults(t *testing.T) {
	s := NewStore()
	e := s.Engine()
	assert.Equal(t, "system", e.Stack)
	assert.NotEmpty(t, e.API.Listen)
}
