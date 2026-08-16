package client

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type stubFinder struct{}

func (stubFinder) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return nil
}

func TestWithAnonymousDiscovery(t *testing.T) {
	finder := stubFinder{}

	var cfg sessionConfig
	WithAnonymousDiscovery(finder)(&cfg)

	require.True(t, cfg.noBroadcast)
	require.Equal(t, finder, cfg.providerFinder)
}
