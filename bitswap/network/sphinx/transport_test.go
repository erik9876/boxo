package sphinx

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// relayNode is one fully wired transport node: host, key manager, relay,
// transport, and a delivery handler
type relayNode struct {
	host      host.Host
	km        *KeyManager
	surbs     *SURBStore
	transport *Transport
	handler   *transportHandler
}

func newRelayNode(t *testing.T) *relayNode {
	return newRelayNodeWithFinder(t, nil)
}

func newRelayNodeWithFinder(t *testing.T, finder PeerFinder) *relayNode {
	t.Helper()
	h := newTestHost(t)
	idPriv := h.Peerstore().PrivKey(h.ID())
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	surbs := NewSURBStore()
	handler := &transportHandler{}
	relay, err := NewRelay(km, surbs, handler)
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	tr := NewTransport(h, relay, finder)
	t.Cleanup(func() { tr.Close() })
	node := &relayNode{host: h, km: km, surbs: surbs, transport: tr, handler: handler}
	handler.node = node
	return node
}

func (n *relayNode) keyInfo() KeyInfo {
	return KeyInfo{PeerID: n.km.PeerID(), PublicKey: n.km.PublicKey()}
}

// transportHandler captures deliveries. When replySURB is set, it answers a
// forward delivery through that SURB, standing in for the proxy, which builds the reply
// from a SURB carried in the forward payload
type transportHandler struct {
	node *relayNode

	mu        sync.Mutex
	delivered [][]byte
	replies   []capturedReply
	replyDone chan struct{}

	replySURB    []byte
	replyPayload []byte
}

func (h *transportHandler) HandleDelivery(_ RecipientID, payload []byte) {
	h.mu.Lock()
	h.delivered = append(h.delivered, append([]byte(nil), payload...))
	surb, reply := h.replySURB, h.replyPayload
	h.mu.Unlock()

	if surb == nil {
		return
	}
	// Answer through the SURB on a fresh outbound stream, exactly as a proxy
	// would. The exit learns only the first return hop
	pkt, first, err := NewReplyFromSURB(surb, reply)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = h.node.transport.SendPacket(ctx, first, pkt)
}

func (h *transportHandler) HandleSURBReply(id SURBID, payload []byte) {
	h.mu.Lock()
	h.replies = append(h.replies, capturedReply{id: id, payload: append([]byte(nil), payload...)})
	if h.replyDone != nil {
		close(h.replyDone)
		h.replyDone = nil
	}
	h.mu.Unlock()
}

func (h *transportHandler) snapshot() ([][]byte, []capturedReply) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]byte(nil), h.delivered...), append([]capturedReply(nil), h.replies...)
}

// connectAll dials every pair so the relay pool's "prior contact" holds and
// forwarding never has to route through the DHT
func connectAll(t *testing.T, hosts ...host.Host) {
	t.Helper()
	for i := range hosts {
		for j := i + 1; j < len(hosts); j++ {
			connect(t, hosts[i], hosts[j])
		}
	}
}

func TestTransportForwardAndSURBRoundtrip(t *testing.T) {
	// Five hosts, reused across forward and return paths
	initiator := newRelayNode(t)
	a := newRelayNode(t)
	b := newRelayNode(t)
	exit := newRelayNode(t)
	c := newRelayNode(t)

	connectAll(t, initiator.host, a.host, b.host, exit.host, c.host)

	// Return path ends at the initiator; reuse host a as a return relay
	surb, id, keys, err := NewSURB([]KeyInfo{c.keyInfo(), a.keyInfo(), initiator.keyInfo()})
	if err != nil {
		t.Fatalf("NewSURB: %v", err)
	}
	initiator.surbs.Put(id, keys)

	replyPayload := []byte("provider record for the requested cid")
	forwardPayload := []byte("anonymous want-have for a cid")

	// Arm the exit: on delivery, reply through the SURB
	done := make(chan struct{})
	exit.handler.mu.Lock()
	exit.handler.replySURB = surb
	exit.handler.replyPayload = replyPayload
	exit.handler.mu.Unlock()
	initiator.handler.mu.Lock()
	initiator.handler.replyDone = done
	initiator.handler.mu.Unlock()

	// Forward path: initiator -> a -> b -> exit (terminal delivery)
	pkt, err := NewForwardPacket([]KeyInfo{a.keyInfo(), b.keyInfo(), exit.keyInfo()}, RecipientID{}, forwardPayload)
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := initiator.transport.SendPacket(ctx, a.km.PeerID(), pkt); err != nil {
		t.Fatalf("originating forward packet: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("initiator never received the SURB reply")
	}

	delivered, _ := exit.handler.snapshot()
	if len(delivered) != 1 || !bytes.Equal(delivered[0], forwardPayload) {
		t.Fatalf("exit delivered %d payloads (want 1, %q)", len(delivered), forwardPayload)
	}

	_, replies := initiator.handler.snapshot()
	if len(replies) != 1 {
		t.Fatalf("initiator got %d replies, want 1", len(replies))
	}
	if replies[0].id != id {
		t.Errorf("reply id = %x, want %x", replies[0].id, id)
	}
	if !bytes.Equal(replies[0].payload, replyPayload) {
		t.Errorf("reply payload = %q, want %q", replies[0].payload, replyPayload)
	}
	if initiator.surbs.Len() != 0 {
		t.Errorf("SURB entry survived a successful reply; %d left", initiator.surbs.Len())
	}
}

func TestTransportDialFailureIsCleanDrop(t *testing.T) {
	sender := newRelayNode(t)

	// A peer the sender has never heard of and cannot reach
	_, unknown := newIdentity(t)

	pkt, err := NewForwardPacket(newHopInfos(t, NrHops), RecipientID{}, []byte("goes nowhere"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sender.transport.SendPacket(ctx, unknown, pkt); !errors.Is(err, ErrUndeliverable) {
		t.Fatalf("SendPacket to unreachable peer: err = %v, want ErrUndeliverable", err)
	}

	// Nothing is torn down: a valid forward terminating at sender still
	// gets delivered. r2 originates by dialing r1, the first hop
	r1 := newRelayNode(t)
	r2 := newRelayNode(t)
	connectAll(t, sender.host, r1.host, r2.host)

	payload := []byte("still alive")
	fwd, err := NewForwardPacket([]KeyInfo{r1.keyInfo(), r2.keyInfo(), sender.keyInfo()}, RecipientID{}, payload)
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := r2.transport.SendPacket(ctx2, r1.km.PeerID(), fwd); err != nil {
		t.Fatalf("originating forward after dial failure: %v", err)
	}

	waitForDelivery(t, sender.handler, payload)
}

// waitForDelivery polls h until it has delivered want, or fails the test
func waitForDelivery(t *testing.T, h *transportHandler, want []byte) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("payload %q was never delivered", want)
		case <-tick.C:
			delivered, _ := h.snapshot()
			for _, d := range delivered {
				if bytes.Equal(d, want) {
					return
				}
			}
		}
	}
}

// recordingFinder is a PeerFinder double: counts calls, serves addresses
// from a fixed map, optionally fails every lookup
type recordingFinder struct {
	mu    sync.Mutex
	calls []peer.ID
	addrs map[peer.ID]peer.AddrInfo
	err   error
}

func (f *recordingFinder) FindPeer(_ context.Context, p peer.ID) (peer.AddrInfo, error) {
	f.mu.Lock()
	f.calls = append(f.calls, p)
	f.mu.Unlock()
	if f.err != nil {
		return peer.AddrInfo{}, f.err
	}
	return f.addrs[p], nil
}

func (f *recordingFinder) lookups() []peer.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]peer.ID(nil), f.calls...)
}

func TestTransportLookupPrecedesEverySend(t *testing.T) {
	// receiver is already connected; the lookup must run anyway
	finder := &recordingFinder{}
	sender := newRelayNodeWithFinder(t, finder)
	receiver := newRelayNode(t)
	connectAll(t, sender.host, receiver.host)

	pkt, err := NewForwardPacket(newHopInfos(t, NrHops), RecipientID{}, []byte("uniform"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sender.transport.SendPacket(ctx, receiver.host.ID(), pkt); err != nil {
		t.Fatalf("SendPacket to connected peer: %v", err)
	}

	calls := finder.lookups()
	if len(calls) != 1 || calls[0] != receiver.host.ID() {
		t.Fatalf("finder calls = %v, want exactly one for %s", calls, receiver.host.ID())
	}
	m := sender.transport.Metrics()
	if m.Lookups != 1 || m.LookupFailures != 0 {
		t.Errorf("metrics = %+v, want 1 lookup, 0 failures", m)
	}
}

func TestTransportLookupSuppliesAddresses(t *testing.T) {
	// no prior contact: without the finder this send would be
	// ErrUndeliverable (see TestTransportDialFailureIsCleanDrop)
	receiver := newRelayNode(t)
	finder := &recordingFinder{addrs: map[peer.ID]peer.AddrInfo{
		receiver.host.ID(): {ID: receiver.host.ID(), Addrs: receiver.host.Addrs()},
	}}
	sender := newRelayNodeWithFinder(t, finder)

	pkt, err := NewForwardPacket(newHopInfos(t, NrHops), RecipientID{}, []byte("resolved"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sender.transport.SendPacket(ctx, receiver.host.ID(), pkt); err != nil {
		t.Fatalf("SendPacket with finder-supplied addresses: %v", err)
	}
}

func TestTransportLookupFailureStillDials(t *testing.T) {
	finder := &recordingFinder{err: errors.New("dht unavailable")}
	sender := newRelayNodeWithFinder(t, finder)
	receiver := newRelayNode(t)
	connectAll(t, sender.host, receiver.host)

	pkt, err := NewForwardPacket(newHopInfos(t, NrHops), RecipientID{}, []byte("despite failure"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sender.transport.SendPacket(ctx, receiver.host.ID(), pkt); err != nil {
		t.Fatalf("SendPacket after failed lookup: %v", err)
	}
	m := sender.transport.Metrics()
	if m.Lookups != 1 || m.LookupFailures != 1 {
		t.Errorf("metrics = %+v, want 1 lookup, 1 failure", m)
	}
}

func TestTransportMalformedInboundRejected(t *testing.T) {
	receiver := newRelayNode(t)
	sender := newTestHost(t)
	connect(t, sender, receiver.host)

	packetLen := Geometry().PacketLength
	cases := map[string][]byte{
		"truncated": make([]byte, packetLen/2),
		"oversized": make([]byte, packetLen+512),
	}
	for name, raw := range cases {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s, err := sender.NewStream(ctx, receiver.host.ID(), ProtocolRelay)
		if err != nil {
			cancel()
			t.Fatalf("%s: opening raw stream: %v", name, err)
		}
		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := s.Write(raw); err != nil {
			// A truncated write may race the handler's Reset; not a failure
			t.Logf("%s: write returned %v", name, err)
		}
		_ = s.Close()
		cancel()
	}

	// The receiver must not have delivered anything and must still be alive.
	// Give the handler goroutines a moment to run
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			if delivered, replies := receiver.handler.snapshot(); len(delivered) != 0 || len(replies) != 0 {
				t.Fatalf("malformed packets produced %d deliveries, %d replies", len(delivered), len(replies))
			}
			return
		case <-tick.C:
			if delivered, replies := receiver.handler.snapshot(); len(delivered) != 0 || len(replies) != 0 {
				t.Fatalf("malformed packets produced %d deliveries, %d replies", len(delivered), len(replies))
			}
		}
	}
}
