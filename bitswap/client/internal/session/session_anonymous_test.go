package session

import (
	"context"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/stretchr/testify/require"
)

func TestSessionNoBroadcastSuppressesAllBroadcasts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New(false)
	defer notif.Shutdown()
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	// Delays short enough that the idle tick and the periodic search both
	// fire several times inside the test window.
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, 10*time.Millisecond, 50*time.Millisecond, "", nil, true)
	defer session.Close()

	blks := random.BlocksOfSize(4, blockSize)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}

	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

	// Neither the initial wants nor any tick may produce a broadcast.
	select {
	case req := <-fpm.wantReqs:
		t.Fatalf("no-broadcast session broadcast %d want-haves", len(req.cids))
	case <-ctx.Done():
	}
}

func TestSessionNoBroadcastQueriesFinderImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New(false)
	defer notif.Shutdown()
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	// Delays far beyond the test window, so only the want path itself can
	// reach the provider finder.
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Minute, time.Minute, "", nil, true)
	defer session.Close()

	blks := random.BlocksOfSize(1, blockSize)
	_, err := session.GetBlocks(ctx, []cid.Cid{blks[0].Cid()})
	require.NoError(t, err, "error getting blocks")

	select {
	case k := <-fpf.findMorePeersRequested:
		require.Equal(t, blks[0].Cid(), k)
	case <-ctx.Done():
		t.Fatal("no-broadcast session did not query the provider finder on the first want")
	}
}
