package sessionmanager

import (
	"context"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bssession "github.com/ipfs/boxo/bitswap/client/internal/session"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type fakeFinder struct{}

func (fakeFinder) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return nil
}

func TestNewSessionWithOptsReachesFactory(t *testing.T) {
	notif := notifications.New(false)
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}

	var got SessionOpts
	factory := func(
		ctx context.Context,
		sm bssession.SessionManager,
		id uint64,
		sprm bssession.SessionPeerManager,
		sim *bssim.SessionInterestManager,
		pm bssession.PeerManager,
		bpm *bsbpm.BlockPresenceManager,
		notif notifications.PubSub,
		provSearchDelay time.Duration,
		rebroadcastDelay time.Duration,
		self peer.ID,
		opts SessionOpts,
	) Session {
		got = opts
		return sessionFactory(ctx, sm, id, sprm, sim, pm, bpm, notif, provSearchDelay, rebroadcastDelay, self, opts)
	}

	sm := New(factory, sim, peerManagerFactory, bpm, pm, notif, "")
	defer sm.Shutdown()

	finder := fakeFinder{}
	sess := sm.NewSessionWithOpts(t.Context(), time.Second, time.Minute, SessionOpts{
		ProviderFinder: finder,
		NoBroadcast:    true,
	})
	require.NotNil(t, sess)
	require.True(t, got.NoBroadcast)
	require.Equal(t, finder, got.ProviderFinder)
}
