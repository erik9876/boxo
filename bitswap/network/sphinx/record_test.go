package sphinx

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/network/sphinx/pb"
	"github.com/katzenpost/hpqc/nike"
	"github.com/katzenpost/hpqc/nike/x25519"
	"github.com/katzenpost/hpqc/rand"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	"google.golang.org/protobuf/proto"
)

// newIdentity returns a fresh libp2p ed25519 identity key and its peer ID
func newIdentity(t *testing.T) (crypto.PrivKey, peer.ID) {
	t.Helper()
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generating identity key: %v", err)
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("deriving peer id: %v", err)
	}
	return priv, pid
}

// newSphinxKey returns a fresh X25519 mix public key
func newSphinxKey(t *testing.T) nike.PublicKey {
	t.Helper()
	pub, _, err := x25519.Scheme(rand.Reader).GenerateKeyPair()
	if err != nil {
		t.Fatalf("generating sphinx key: %v", err)
	}
	return pub
}

// newSignedRecord builds a KeyRecord and the identity key that should sign it
func newSignedRecord(t *testing.T, ttl time.Duration) (*KeyRecord, crypto.PrivKey) {
	t.Helper()
	idPriv, pid := newIdentity(t)
	rec := NewKeyRecord(pid, newSphinxKey(t), ttl)
	return rec, idPriv
}

// sealToBytes seals rec with idPriv and returns the marshaled envelope
func sealToBytes(t *testing.T, rec *KeyRecord, idPriv crypto.PrivKey) []byte {
	t.Helper()
	env, err := record.Seal(rec, idPriv)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("Marshal envelope: %v", err)
	}
	return raw
}

func TestKeyRecordMarshalRoundtrip(t *testing.T) {
	rec, _ := newSignedRecord(t, time.Hour)

	raw, err := rec.MarshalRecord()
	if err != nil {
		t.Fatalf("MarshalRecord: %v", err)
	}

	// The payload must be the pb.KeyRecord protobuf, nothing else
	var msg pb.KeyRecord
	if err := proto.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("payload is not a pb.KeyRecord: %v", err)
	}

	var got KeyRecord
	if err := got.UnmarshalRecord(raw); err != nil {
		t.Fatalf("UnmarshalRecord: %v", err)
	}
	assertRecordsEqual(t, rec, &got)
}

func TestKeyRecordSealConsumeRoundtrip(t *testing.T) {
	rec, idPriv := newSignedRecord(t, time.Hour)
	raw := sealToBytes(t, rec, idPriv)

	got, err := ConsumeKeyRecord(raw)
	if err != nil {
		t.Fatalf("ConsumeKeyRecord: %v", err)
	}
	assertRecordsEqual(t, rec, got)
}

func TestKeyRecordWrongSignerRejected(t *testing.T) {
	// The record claims one peer but is sealed by a different identity: the
	// signer binding in ConsumeKeyRecord must reject it
	rec, _ := newSignedRecord(t, time.Hour)
	otherPriv, _ := newIdentity(t)
	raw := sealToBytes(t, rec, otherPriv)

	if _, err := ConsumeKeyRecord(raw); err == nil {
		t.Fatal("ConsumeKeyRecord accepted a record sealed by a different identity")
	}
}

func TestKeyRecordExpiredRejectedOnConsume(t *testing.T) {
	rec, idPriv := newSignedRecord(t, -time.Minute) // already expired
	raw := sealToBytes(t, rec, idPriv)

	if _, err := ConsumeKeyRecord(raw); err == nil {
		t.Fatal("ConsumeKeyRecord accepted an expired record")
	}
}

func TestKeyRecordTamperedEnvelopeRejected(t *testing.T) {
	rec, idPriv := newSignedRecord(t, time.Hour)
	raw := sealToBytes(t, rec, idPriv)

	// Flip a payload byte and re-serialize with the original signature
	tampered, err := record.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	if len(tampered.RawPayload) == 0 {
		t.Fatal("envelope payload unexpectedly empty")
	}
	tampered.RawPayload[0] ^= 0xFF
	tamperedRaw, err := tampered.Marshal()
	if err != nil {
		t.Fatalf("Marshal tampered envelope: %v", err)
	}

	if _, err := ConsumeKeyRecord(tamperedRaw); !errors.Is(err, record.ErrInvalidSignature) {
		t.Fatalf("ConsumeKeyRecord on tampered bytes: got %v, want ErrInvalidSignature", err)
	}
}

func TestKeyRecordWrongDomainRejected(t *testing.T) {
	rec, idPriv := newSignedRecord(t, time.Hour)
	raw := sealToBytes(t, rec, idPriv)

	if _, _, err := record.ConsumeEnvelope(raw, "some-other-domain"); err == nil {
		t.Fatal("ConsumeEnvelope with wrong domain: got nil error, want failure")
	}
}

func TestKeyRecordUnmarshalRejectsBadPublicKey(t *testing.T) {
	rec, _ := newSignedRecord(t, time.Hour)
	rec.SphinxPublicKey = rec.SphinxPublicKey[:x25519.PublicKeySize-1] // truncate

	raw, err := rec.MarshalRecord()
	if err != nil {
		t.Fatalf("MarshalRecord: %v", err)
	}
	var got KeyRecord
	if err := got.UnmarshalRecord(raw); err == nil {
		t.Fatal("UnmarshalRecord accepted a wrong-length public key")
	}
}

func TestKeyRecordUnmarshalRejectsBadPeerID(t *testing.T) {
	raw, err := proto.Marshal(&pb.KeyRecord{
		PeerId:         []byte("not a peer id"),
		PublicKey:      make([]byte, x25519.PublicKeySize),
		Seq:            1,
		ExpiryUnixNano: time.Now().Add(time.Hour).UnixNano(),
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var got KeyRecord
	if err := got.UnmarshalRecord(raw); err == nil {
		t.Fatal("UnmarshalRecord accepted an unparseable peer id")
	}
}

func assertRecordsEqual(t *testing.T, want, got *KeyRecord) {
	t.Helper()
	if got.PeerID != want.PeerID {
		t.Errorf("PeerID = %s, want %s", got.PeerID, want.PeerID)
	}
	if !bytes.Equal(got.SphinxPublicKey, want.SphinxPublicKey) {
		t.Errorf("SphinxPublicKey mismatch")
	}
	if got.Seq != want.Seq {
		t.Errorf("Seq = %d, want %d", got.Seq, want.Seq)
	}
	if !got.Expiry.Equal(want.Expiry) {
		t.Errorf("Expiry = %v, want %v", got.Expiry, want.Expiry)
	}
}
