package sphinx

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

// testNode is one in-memory Sphinx node: keypair, relay, SURB store, and a
// capturing delivery handler. No network involved
type testNode struct {
	km      *KeyManager
	relay   *Relay
	surbs   *SURBStore
	handler *captureHandler
}

func newTestNode(t *testing.T) *testNode {
	t.Helper()
	idPriv, _ := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	surbs := NewSURBStore()
	handler := &captureHandler{}
	relay, err := NewRelay(km, surbs, handler)
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	return &testNode{km: km, relay: relay, surbs: surbs, handler: handler}
}

// keyInfo returns the node's pool entry as path building consumes it
func (n *testNode) keyInfo() KeyInfo {
	return KeyInfo{PeerID: n.km.PeerID(), PublicKey: n.km.PublicKey()}
}

// captureHandler records everything the relay delivers locally
type captureHandler struct {
	mu         sync.Mutex
	delivered  [][]byte
	recipients []RecipientID
	replies    []capturedReply
}

type capturedReply struct {
	id      SURBID
	payload []byte
}

func (h *captureHandler) HandleDelivery(recipient RecipientID, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.delivered = append(h.delivered, append([]byte(nil), payload...))
	h.recipients = append(h.recipients, recipient)
}

func (h *captureHandler) HandleSURBReply(id SURBID, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replies = append(h.replies, capturedReply{id: id, payload: append([]byte(nil), payload...)})
}

func (h *captureHandler) snapshot() ([][]byte, []capturedReply) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]byte(nil), h.delivered...), append([]capturedReply(nil), h.replies...)
}

// hopThrough processes pkt at each node in turn, asserting that every node
// but the last forwards to its successor and that packet size never changes.
// It returns whatever the last node's ProcessPacket returned
func hopThrough(t *testing.T, pkt []byte, nodes ...*testNode) (*Forward, error) {
	t.Helper()
	packetLen := Geometry().PacketLength
	for i, n := range nodes[:len(nodes)-1] {
		fwd, err := n.relay.ProcessPacket(pkt)
		if err != nil {
			t.Fatalf("hop %d: ProcessPacket: %v", i, err)
		}
		if fwd == nil {
			t.Fatalf("hop %d: packet was consumed, want forward", i)
		}
		if want := nodes[i+1].km.PeerID(); fwd.Next != want {
			t.Fatalf("hop %d: forwards to %s, want %s", i, fwd.Next, want)
		}
		if len(fwd.Packet) != packetLen {
			t.Fatalf("hop %d: forwarded packet is %d bytes, want %d", i, len(fwd.Packet), packetLen)
		}
		pkt = fwd.Packet
	}
	return nodes[len(nodes)-1].relay.ProcessPacket(pkt)
}

func TestForwardRoundtrip(t *testing.T) {
	hop1, hop2, exit := newTestNode(t), newTestNode(t), newTestNode(t)
	payload := []byte("anonymous discovery job payload")

	pkt, err := NewForwardPacket([]KeyInfo{hop1.keyInfo(), hop2.keyInfo(), exit.keyInfo()}, DiscoveryRecipient, payload)
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	if len(pkt) != Geometry().PacketLength {
		t.Fatalf("emitted packet is %d bytes, want %d", len(pkt), Geometry().PacketLength)
	}

	fwd, err := hopThrough(t, pkt, hop1, hop2, exit)
	if err != nil {
		t.Fatalf("exit: ProcessPacket: %v", err)
	}
	if fwd != nil {
		t.Fatal("exit forwarded the packet instead of delivering it")
	}

	delivered, _ := exit.handler.snapshot()
	if len(delivered) != 1 {
		t.Fatalf("exit delivered %d payloads, want 1", len(delivered))
	}
	if !bytes.Equal(delivered[0], payload) {
		t.Errorf("delivered payload = %q, want %q", delivered[0], payload)
	}
	exit.handler.mu.Lock()
	gotRecipient := exit.handler.recipients[0]
	exit.handler.mu.Unlock()
	if gotRecipient != DiscoveryRecipient {
		t.Errorf("delivered recipient = %x, want DiscoveryRecipient", gotRecipient)
	}
	for name, n := range map[string]*testNode{"hop1": hop1, "hop2": hop2} {
		d, r := n.handler.snapshot()
		if len(d) != 0 || len(r) != 0 {
			t.Errorf("%s consumed a payload locally: %d deliveries, %d replies", name, len(d), len(r))
		}
	}
}

func TestSURBRoundtrip(t *testing.T) {
	initiator := newTestNode(t)
	ret1, ret2 := newTestNode(t), newTestNode(t)

	// The initiator prepares the reply envelope: a SURB over a return path
	// that ends at itself, keys stored under the SURB ID
	surb, id, keys, err := NewSURB([]KeyInfo{ret1.keyInfo(), ret2.keyInfo(), initiator.keyInfo()})
	if err != nil {
		t.Fatalf("NewSURB: %v", err)
	}
	initiator.surbs.Put(id, keys)

	// The responder answers through the SURB, learning only the first hop
	payload := []byte("provider record for the requested cid")
	reply, first, err := NewReplyFromSURB(surb, payload)
	if err != nil {
		t.Fatalf("NewReplyFromSURB: %v", err)
	}
	if first != ret1.km.PeerID() {
		t.Fatalf("reply enters at %s, want %s", first, ret1.km.PeerID())
	}
	if len(reply) != Geometry().PacketLength {
		t.Fatalf("reply packet is %d bytes, want %d", len(reply), Geometry().PacketLength)
	}

	fwd, err := hopThrough(t, reply, ret1, ret2, initiator)
	if err != nil {
		t.Fatalf("initiator: ProcessPacket: %v", err)
	}
	if fwd != nil {
		t.Fatal("initiator forwarded the reply instead of consuming it")
	}

	_, replies := initiator.handler.snapshot()
	if len(replies) != 1 {
		t.Fatalf("initiator got %d SURB replies, want 1", len(replies))
	}
	if replies[0].id != id {
		t.Errorf("reply matched SURB id %x, want %x", replies[0].id, id)
	}
	if !bytes.Equal(replies[0].payload, payload) {
		t.Errorf("reply payload = %q, want %q", replies[0].payload, payload)
	}
	if initiator.surbs.Len() != 0 {
		t.Errorf("SURB entry survived a successful reply; store holds %d", initiator.surbs.Len())
	}
}

func TestReplayRejected(t *testing.T) {
	hop1, hop2, exit := newTestNode(t), newTestNode(t), newTestNode(t)

	pkt, err := NewForwardPacket([]KeyInfo{hop1.keyInfo(), hop2.keyInfo(), exit.keyInfo()}, RecipientID{}, []byte("once"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	replayed := append([]byte(nil), pkt...) // ProcessPacket mutates in place

	if _, err := hop1.relay.ProcessPacket(pkt); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if _, err := hop1.relay.ProcessPacket(replayed); !errors.Is(err, ErrReplay) {
		t.Fatalf("second delivery: err = %v, want ErrReplay", err)
	}
}

func TestTamperedHeaderRejected(t *testing.T) {
	hop1, hop2, exit := newTestNode(t), newTestNode(t), newTestNode(t)

	pkt, err := NewForwardPacket([]KeyInfo{hop1.keyInfo(), hop2.keyInfo(), exit.keyInfo()}, RecipientID{}, []byte("intact"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}
	original := append([]byte(nil), pkt...)

	// Flip one bit in the routing info region of the header
	pkt[40] ^= 0x01
	if _, err := hop1.relay.ProcessPacket(pkt); err == nil {
		t.Fatal("tampered header was accepted")
	} else if errors.Is(err, ErrReplay) {
		t.Fatalf("tampered header reported as replay: %v", err)
	}

	// The failed unwrap must not have burned the genuine packet's tag
	if _, err := hop1.relay.ProcessPacket(original); err != nil {
		t.Fatalf("genuine packet rejected after tampered copy: %v", err)
	}
}

func TestTamperedPayloadRejectedAtExit(t *testing.T) {
	hop1, hop2, exit := newTestNode(t), newTestNode(t), newTestNode(t)

	pkt, err := NewForwardPacket([]KeyInfo{hop1.keyInfo(), hop2.keyInfo(), exit.keyInfo()}, RecipientID{}, []byte("intact"))
	if err != nil {
		t.Fatalf("NewForwardPacket: %v", err)
	}

	// Intermediate hops cannot see payload integrity; the tampering must
	// surface at the exit, where the payload tag is verified
	pkt[Geometry().HeaderLength+100] ^= 0x01
	fwd, err := hop1.relay.ProcessPacket(pkt)
	if err != nil {
		t.Fatalf("hop1: %v", err)
	}
	fwd, err = hop2.relay.ProcessPacket(fwd.Packet)
	if err != nil {
		t.Fatalf("hop2: %v", err)
	}
	if _, err := exit.relay.ProcessPacket(fwd.Packet); err == nil {
		t.Fatal("exit accepted a packet with tampered payload")
	}
	if delivered, _ := exit.handler.snapshot(); len(delivered) != 0 {
		t.Errorf("exit delivered %d tampered payloads", len(delivered))
	}
}

func TestUnknownSURBReplyRejected(t *testing.T) {
	initiator := newTestNode(t)
	ret1, ret2 := newTestNode(t), newTestNode(t)

	// The initiator never stores the keys, as if the SURB had already been
	// consumed by an earlier reply
	surb, _, _, err := NewSURB([]KeyInfo{ret1.keyInfo(), ret2.keyInfo(), initiator.keyInfo()})
	if err != nil {
		t.Fatalf("NewSURB: %v", err)
	}
	reply, _, err := NewReplyFromSURB(surb, []byte("late"))
	if err != nil {
		t.Fatalf("NewReplyFromSURB: %v", err)
	}

	fwd, err := ret1.relay.ProcessPacket(reply)
	if err != nil {
		t.Fatalf("ret1: %v", err)
	}
	fwd, err = ret2.relay.ProcessPacket(fwd.Packet)
	if err != nil {
		t.Fatalf("ret2: %v", err)
	}
	if _, err := initiator.relay.ProcessPacket(fwd.Packet); !errors.Is(err, ErrUnknownSURB) {
		t.Fatalf("err = %v, want ErrUnknownSURB", err)
	}
	if _, replies := initiator.handler.snapshot(); len(replies) != 0 {
		t.Errorf("handler got %d replies without stored keys", len(replies))
	}
}

func TestWrongSizePacketRejected(t *testing.T) {
	node := newTestNode(t)
	if _, err := node.relay.ProcessPacket(make([]byte, 100)); err == nil {
		t.Fatal("undersized packet was accepted")
	}
	if _, err := node.relay.ProcessPacket(make([]byte, Geometry().PacketLength+1)); err == nil {
		t.Fatal("oversized packet was accepted")
	}
}
