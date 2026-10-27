package config

import (
	"encoding/json"
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

	// outbound-level upstream routes survive the store round-trip
	s.SetOutbound(&OutboundSpec{ID: "ob2", Protocol: "http", UpstreamProxy: "pool:p1",
		Config: map[string]any{"address": "1.2.3.4:8080"}})
	ob2, ok := s.Outbound("ob2")
	require.True(t, ok)
	assert.Equal(t, "pool:p1", ob2.UpstreamProxy)

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

// TestOutboundUpstreamProxyJSONRoundTrip: the outbound-level upstream route
// survives the durable-config wire format under its documented
// "upstream_proxy" key, both directions.
func TestOutboundUpstreamProxyJSONRoundTrip(t *testing.T) {
	doc := Doc{
		Outbounds: []*OutboundSpec{
			{ID: "ob", Protocol: "http", UpstreamProxy: "chain:c1",
				Config: map[string]any{"address": "1.2.3.4:8080"}},
		},
	}
	b, err := marshalDoc(doc)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"upstream_proxy": "chain:c1"`, "wire key should be upstream_proxy")

	var back Doc
	require.NoError(t, json.Unmarshal(b, &back))
	require.Len(t, back.Outbounds, 1)
	assert.Equal(t, "chain:c1", back.Outbounds[0].UpstreamProxy)
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

func TestStoreEngineDefaultsAPIListenEnv(t *testing.T) {
	// Unset: the documented default is loopback.
	t.Setenv("GOOSE_API_LISTEN", "")
	assert.Equal(t, "127.0.0.1:9090", NewStore().Engine().API.Listen)

	// Set: overrides the default (containerized runs bind 0.0.0.0).
	t.Setenv("GOOSE_API_LISTEN", "0.0.0.0:9090")
	assert.Equal(t, "0.0.0.0:9090", NewStore().Engine().API.Listen)
}
