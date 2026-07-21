package bitswap_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	testinstance "github.com/ipfs/boxo/bitswap/testinstance"
	tn "github.com/ipfs/boxo/bitswap/testnet"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-test/random"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type receivedMsg struct {
	from peer.ID
	msg  bsmsg.BitSwapMessage
}

// recordingReceiver is a network.Receiver that records every message
type recordingReceiver struct {
	seen chan receivedMsg
}

func (r *recordingReceiver) ReceiveMessage(_ context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	r.seen <- receivedMsg{from: sender, msg: incoming}
}

func (r *recordingReceiver) ReceiveError(error)      {}
func (r *recordingReceiver) PeerConnected(peer.ID)   {}
func (r *recordingReceiver) PeerDisconnected(peer.ID) {}

// An extra receiver passed to bitswap.New must see inbound messages
// alongside the client and server
func TestWithExtraReceiversSeesInboundMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	vnet := tn.VirtualNetwork(delay.Fixed(0))
	router := mockrouting.NewServer()
	rec := &recordingReceiver{seen: make(chan receivedMsg, 8)}
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil,
		[]bitswap.Option{bitswap.WithExtraReceivers(rec)})
	defer ig.Close()

	insts := ig.Instances(2)
	a, b := insts[0], insts[1]

	c := random.BlocksOfSize(1, 4)[0].Cid()
	msg := bsmsg.New(false)
	msg.AddEntry(c, 1, pb.Message_Wantlist_Have, false)
	if err := a.Adapter.SendMessage(ctx, b.Identity.ID(), msg); err != nil {
		t.Fatalf("sending message: %v", err)
	}

	select {
	case got := <-rec.seen:
		if got.from != a.Identity.ID() {
			t.Fatalf("message from %s, want %s", got.from, a.Identity.ID())
		}
		wl := got.msg.Wantlist()
		if len(wl) != 1 || !wl[0].Cid.Equals(c) || wl[0].WantType != pb.Message_Wantlist_Have {
			t.Fatalf("unexpected wantlist: %v", wl)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("extra receiver never saw the message")
	}
}

// keep the interface honest at compile time
var _ bsnet.Receiver = (*recordingReceiver)(nil)
