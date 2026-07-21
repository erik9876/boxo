package sphinx

import (
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network/sphinx/pb"
	"github.com/katzenpost/hpqc/nike"
	"github.com/katzenpost/hpqc/nike/x25519"
	"github.com/katzenpost/hpqc/rand"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	"google.golang.org/protobuf/proto"
)

// lets record.ConsumeEnvelope unmarshal our payload type into *KeyRecord
func init() {
	record.RegisterType(&KeyRecord{})
}

// signature domain; prevents signature replay across record types
const KeyRecordEnvelopeDomain = "sphinx-key-record"

// payload type hint inside the envelope
var KeyRecordEnvelopePayloadType = []byte("Sp")

// KeyRecord binds a peer identity to its X25519 Sphinx key, with a
// strictly increasing Seq and an absolute expiry. Travels in a
// record.Envelope signed by the peer's identity key; ConsumeKeyRecord
// verifies the binding
type KeyRecord struct {
	PeerID peer.ID

	// raw X25519 key, exactly x25519.PublicKeySize bytes
	SphinxPublicKey []byte

	// orders records from the same peer; higher wins
	Seq uint64

	Expiry time.Time
}

func NewKeyRecord(pid peer.ID, sphinxPub nike.PublicKey, ttl time.Duration) *KeyRecord {
	return &KeyRecord{
		PeerID:          pid,
		SphinxPublicKey: sphinxPub.Bytes(),
		Seq:             TimestampSeq(),
		Expiry:          time.Now().Add(ttl),
	}
}

var (
	lastTimestampMu sync.Mutex
	lastTimestamp   uint64
)

// TimestampSeq returns a strictly increasing timestamp-based sequence
// number; never returns the same value twice even if the clock stalls
func TimestampSeq() uint64 {
	now := uint64(time.Now().UnixNano())
	lastTimestampMu.Lock()
	defer lastTimestampMu.Unlock()
	if now <= lastTimestamp {
		now = lastTimestamp + 1
	}
	lastTimestamp = now
	return now
}

// ConsumeKeyRecord validates raw: envelope signature and domain, signer
// bound to the record's PeerID, not expired. The only entry point for
// records from the network
func ConsumeKeyRecord(raw []byte) (*KeyRecord, error) {
	env, untyped, err := record.ConsumeEnvelope(raw, KeyRecordEnvelopeDomain)
	if err != nil {
		return nil, err
	}
	rec, ok := untyped.(*KeyRecord)
	if !ok {
		return nil, fmt.Errorf("envelope payload has type %T, want *KeyRecord", untyped)
	}
	signer, err := peer.IDFromPublicKey(env.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("deriving signer peer id: %w", err)
	}
	if signer != rec.PeerID {
		return nil, fmt.Errorf("envelope signed by %s but record claims %s", signer, rec.PeerID)
	}
	if rec.IsExpired(time.Now()) {
		return nil, fmt.Errorf("KeyRecord for %s expired at %s", rec.PeerID, rec.Expiry)
	}
	return rec, nil
}

func (r *KeyRecord) IsExpired(now time.Time) bool {
	return now.After(r.Expiry)
}

// parses on every call; the KeyStore caches the parsed form
func (r *KeyRecord) PublicKey() (nike.PublicKey, error) {
	return x25519.Scheme(rand.Reader).UnmarshalBinaryPublicKey(r.SphinxPublicKey)
}

// Domain implements record.Record
func (r *KeyRecord) Domain() string {
	return KeyRecordEnvelopeDomain
}

// Codec implements record.Record
func (r *KeyRecord) Codec() []byte {
	return KeyRecordEnvelopePayloadType
}

// MarshalRecord implements record.Record; pb.KeyRecord is the wire format
func (r *KeyRecord) MarshalRecord() ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("cannot marshal nil KeyRecord")
	}
	return proto.Marshal(&pb.KeyRecord{
		PeerId:         []byte(r.PeerID),
		PublicKey:      r.SphinxPublicKey,
		Seq:            r.Seq,
		ExpiryUnixNano: r.Expiry.UnixNano(),
	})
}

// UnmarshalRecord implements record.Record; rejects unparseable peer IDs
// and wrong-length keys
func (r *KeyRecord) UnmarshalRecord(data []byte) error {
	if r == nil {
		return fmt.Errorf("cannot unmarshal KeyRecord to nil receiver")
	}
	var msg pb.KeyRecord
	if err := proto.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshaling KeyRecord payload: %w", err)
	}
	pid, err := peer.IDFromBytes(msg.PeerId)
	if err != nil {
		return fmt.Errorf("parsing peer id: %w", err)
	}
	if len(msg.PublicKey) != x25519.PublicKeySize {
		return fmt.Errorf("sphinx public key is %d bytes, want %d", len(msg.PublicKey), x25519.PublicKeySize)
	}
	r.PeerID = pid
	r.SphinxPublicKey = msg.PublicKey
	r.Seq = msg.Seq
	r.Expiry = time.Unix(0, msg.ExpiryUnixNano)
	return nil
}
