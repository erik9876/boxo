package sphinx

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNodeIDPeerIDRoundtrip(t *testing.T) {
	_, pid := newIdentity(t)

	id, err := NodeIDFromPeer(pid)
	if err != nil {
		t.Fatalf("NodeIDFromPeer: %v", err)
	}

	// The NodeID must be the raw identity key, not a digest of it
	pk, err := pid.ExtractPublicKey()
	if err != nil {
		t.Fatalf("ExtractPublicKey: %v", err)
	}
	raw, err := pk.Raw()
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if !bytes.Equal(id[:], raw) {
		t.Error("NodeID differs from the raw ed25519 public key")
	}

	back, err := PeerIDFromNodeID(id)
	if err != nil {
		t.Fatalf("PeerIDFromNodeID: %v", err)
	}
	if back != pid {
		t.Errorf("roundtrip peer id = %s, want %s", back, pid)
	}
}

// nonEd25519PeerID returns a peer ID whose identity is not Ed25519
func nonEd25519PeerID(t *testing.T, typ int, bits int) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPairWithReader(typ, bits, rand.Reader)
	if err != nil {
		t.Fatalf("generating key type %d: %v", typ, err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("deriving peer id: %v", err)
	}
	return pid
}

func TestNodeIDFromPeerRejectsNonEd25519(t *testing.T) {
	// Secp256k1 keys are embedded in the peer ID, so extraction succeeds
	// and the key type check must reject; RSA peer IDs are hashed, so
	// extraction itself fails. Both are the same error to callers
	cases := map[string]peer.ID{
		"secp256k1": nonEd25519PeerID(t, crypto.Secp256k1, 256),
		"rsa":       nonEd25519PeerID(t, crypto.RSA, 2048),
	}
	for name, pid := range cases {
		if _, err := NodeIDFromPeer(pid); !errors.Is(err, ErrNotEd25519) {
			t.Errorf("%s: err = %v, want ErrNotEd25519", name, err)
		}
	}
}
