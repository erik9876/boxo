package sphinx

import (
	"fmt"
	"sync"
	"time"

	"github.com/katzenpost/hpqc/nike"
	"github.com/katzenpost/hpqc/nike/x25519"
	"github.com/katzenpost/hpqc/rand"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
)

// KeyManager holds the node's Sphinx X25519 keypair and serves the sealed
// KeyRecord envelope advertising it. The envelope is cached and re-sealed
// past half its TTL, so peers never get near-expired records
type KeyManager struct {
	identity crypto.PrivKey
	peerID   peer.ID
	pub      nike.PublicKey
	priv     nike.PrivateKey
	ttl      time.Duration

	mu     sync.Mutex
	sealed []byte
	expiry time.Time

	// clock, overridable in tests
	now func() time.Time
}

func NewKeyManager(identity crypto.PrivKey, ttl time.Duration) (*KeyManager, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("record ttl must be positive, got %s", ttl)
	}
	pid, err := peer.IDFromPrivateKey(identity)
	if err != nil {
		return nil, fmt.Errorf("deriving peer id: %w", err)
	}
	pub, priv, err := x25519.Scheme(rand.Reader).GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("generating sphinx keypair: %w", err)
	}
	return &KeyManager{
		identity: identity,
		peerID:   pid,
		pub:      pub,
		priv:     priv,
		ttl:      ttl,
		now:      time.Now,
	}, nil
}

func (km *KeyManager) PeerID() peer.ID { return km.peerID }

func (km *KeyManager) PublicKey() nike.PublicKey { return km.pub }

func (km *KeyManager) PrivateKey() nike.PrivateKey { return km.priv }

// SealedRecord returns the signed envelope bytes advertising the node's
// key. Cached; re-sealed with fresh Seq and Expiry once less than half the
// TTL is left
func (km *KeyManager) SealedRecord() ([]byte, error) {
	km.mu.Lock()
	defer km.mu.Unlock()

	now := km.now()
	if km.sealed != nil && now.Before(km.expiry.Add(-km.ttl/2)) {
		return km.sealed, nil
	}

	rec := &KeyRecord{
		PeerID:          km.peerID,
		SphinxPublicKey: km.pub.Bytes(),
		Seq:             TimestampSeq(),
		Expiry:          now.Add(km.ttl),
	}
	env, err := record.Seal(rec, km.identity)
	if err != nil {
		return nil, fmt.Errorf("sealing key record: %w", err)
	}
	raw, err := env.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshaling key record envelope: %w", err)
	}
	km.sealed = raw
	km.expiry = rec.Expiry
	return raw, nil
}
