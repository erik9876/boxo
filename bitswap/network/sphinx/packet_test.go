package sphinx

import (
	"bytes"
	"testing"
	"time"

	"github.com/katzenpost/katzenpost/core/sphinx/commands"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// newHopInfos returns n KeyInfo entries backed by fresh KeyManagers, as the
// KeyStore would hand them out
func newHopInfos(t *testing.T, n int) []KeyInfo {
	t.Helper()
	infos := make([]KeyInfo, n)
	for i := range infos {
		idPriv, pid := newIdentity(t)
		km, err := NewKeyManager(idPriv, time.Hour)
		if err != nil {
			t.Fatalf("NewKeyManager: %v", err)
		}
		infos[i] = KeyInfo{PeerID: pid, PublicKey: km.PublicKey()}
	}
	return infos
}

func TestForwardPathShape(t *testing.T) {
	hops := newHopInfos(t, NrHops)
	path, err := ForwardPath(hops, DiscoveryRecipient)
	if err != nil {
		t.Fatalf("ForwardPath: %v", err)
	}
	for i, hop := range path[:len(path)-1] {
		if len(hop.Commands) != 0 {
			t.Errorf("intermediate hop %d carries %d commands, want 0", i, len(hop.Commands))
		}
	}
	terminal := path[len(path)-1]
	if len(terminal.Commands) != 1 {
		t.Fatalf("terminal hop carries %d commands, want 1", len(terminal.Commands))
	}
	rcpt, ok := terminal.Commands[0].(*commands.Recipient)
	if !ok {
		t.Fatalf("terminal command is %T, want *commands.Recipient", terminal.Commands[0])
	}
	if rcpt.ID != DiscoveryRecipient {
		t.Errorf("Recipient ID = %x, want the caller-chosen recipient", rcpt.ID)
	}
	for i, hop := range path {
		want, err := NodeIDFromPeer(hops[i].PeerID)
		if err != nil {
			t.Fatalf("NodeIDFromPeer: %v", err)
		}
		if hop.ID != want {
			t.Errorf("hop %d ID does not match its peer's raw key", i)
		}
	}
}

func TestReturnPathShape(t *testing.T) {
	hops := newHopInfos(t, NrHops)
	id := SURBID{1, 2, 3, 4}
	path, err := ReturnPath(hops, id)
	if err != nil {
		t.Fatalf("ReturnPath: %v", err)
	}
	terminal := path[len(path)-1]
	if len(terminal.Commands) != 2 {
		t.Fatalf("terminal hop carries %d commands, want 2", len(terminal.Commands))
	}
	if _, ok := terminal.Commands[0].(*commands.Recipient); !ok {
		t.Errorf("first terminal command is %T, want *commands.Recipient", terminal.Commands[0])
	}
	reply, ok := terminal.Commands[1].(*commands.SURBReply)
	if !ok {
		t.Fatalf("second terminal command is %T, want *commands.SURBReply", terminal.Commands[1])
	}
	if reply.ID != id {
		t.Errorf("SURBReply ID = %x, want %x", reply.ID, id)
	}
}

func TestPathRejectsWrongHopCount(t *testing.T) {
	for _, n := range []int{0, NrHops - 1, NrHops + 1} {
		if _, err := ForwardPath(newHopInfos(t, n), RecipientID{}); err == nil {
			t.Errorf("ForwardPath accepted %d hops", n)
		}
	}
}

func TestPathRejectsNonEd25519Hop(t *testing.T) {
	hops := newHopInfos(t, NrHops)
	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generating secp256k1 key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("deriving peer id: %v", err)
	}
	hops[1].PeerID = pid
	if _, err := ForwardPath(hops, RecipientID{}); err == nil {
		t.Fatal("ForwardPath accepted a non-ed25519 hop")
	}
}

func TestPadUnpadRoundtrip(t *testing.T) {
	size := Geometry().ForwardPayloadLength
	for _, payload := range [][]byte{
		nil,
		{},
		[]byte("short"),
		bytes.Repeat([]byte{0xAB}, size-payloadLenPrefix), // maximum
	} {
		padded, err := padPayload(payload, size)
		if err != nil {
			t.Fatalf("padPayload(%d bytes): %v", len(payload), err)
		}
		if len(padded) != size {
			t.Fatalf("padded length = %d, want %d", len(padded), size)
		}
		back, err := unpadPayload(padded)
		if err != nil {
			t.Fatalf("unpadPayload: %v", err)
		}
		if !bytes.Equal(back, payload) {
			t.Errorf("roundtrip of %d bytes altered the payload", len(payload))
		}
	}
}

func TestPadRejectsOversizedPayload(t *testing.T) {
	size := Geometry().ForwardPayloadLength
	if _, err := padPayload(make([]byte, size-payloadLenPrefix+1), size); err == nil {
		t.Fatal("padPayload accepted an oversized payload")
	}
}

func TestUnpadRejectsLyingPrefix(t *testing.T) {
	padded := []byte{0xFF, 0xFF, 0x00, 0x00} // claims 65535 bytes, has 2
	if _, err := unpadPayload(padded); err == nil {
		t.Fatal("unpadPayload accepted a length prefix beyond the buffer")
	}
	if _, err := unpadPayload([]byte{0x01}); err == nil {
		t.Fatal("unpadPayload accepted a buffer shorter than the prefix")
	}
}

func TestNewSURBProperties(t *testing.T) {
	g := Geometry()
	hops := newHopInfos(t, NrHops)
	surb, id, keys, err := NewSURB(hops)
	if err != nil {
		t.Fatalf("NewSURB: %v", err)
	}
	if len(surb) != g.SURBLength {
		t.Errorf("surb length = %d, want %d", len(surb), g.SURBLength)
	}
	// One SPRP key+IV pair per hop plus the payload key
	if want := g.SPRPKeyMaterialLength * (NrHops + 1); len(keys) != want {
		t.Errorf("keys length = %d, want %d", len(keys), want)
	}
	if id == (SURBID{}) {
		t.Error("surb id is all zero; random generation failed")
	}
}
