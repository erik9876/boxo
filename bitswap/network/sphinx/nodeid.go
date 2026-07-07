package sphinx

import (
	"errors"
	"fmt"

	"github.com/katzenpost/katzenpost/core/sphinx/constants"
	"github.com/libp2p/go-libp2p/core/crypto"
	cryptopb "github.com/libp2p/go-libp2p/core/crypto/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NodeID is the routing identifier of a hop: the raw Ed25519 public key of
// its libp2p identity. Raw (not hashed) so the mapping to a peer.ID stays
// reversible and a relay can dial the next hop without prior knowledge
type NodeID = [constants.NodeIDLength]byte

// SURBID identifies a single-use reply block; keys the SURBStore
type SURBID = [constants.SURBIDLength]byte

// RecipientID is the demux field of a terminal Recipient command (see
// DiscoveryRecipient)
type RecipientID = [constants.RecipientIDLength]byte

// non-Ed25519 identity or a hashed peer ID (RSA); cannot serve as a hop
var ErrNotEd25519 = errors.New("peer identity is not an ed25519 key")

// NodeIDFromPeer extracts the raw Ed25519 key embedded in pid
func NodeIDFromPeer(pid peer.ID) (NodeID, error) {
	var id NodeID
	pk, err := pid.ExtractPublicKey()
	if err != nil {
		// hashed peer IDs (e.g. RSA) do not embed their key
		return id, fmt.Errorf("no key embedded in peer id %s (%v): %w", pid, err, ErrNotEd25519)
	}
	if pk.Type() != cryptopb.KeyType_Ed25519 {
		return id, fmt.Errorf("peer %s has key type %s: %w", pid, pk.Type(), ErrNotEd25519)
	}
	raw, err := pk.Raw()
	if err != nil {
		return id, fmt.Errorf("serializing public key of %s: %w", pid, err)
	}
	if len(raw) != constants.NodeIDLength {
		return id, fmt.Errorf("ed25519 key of %s is %d bytes, want %d", pid, len(raw), constants.NodeIDLength)
	}
	copy(id[:], raw)
	return id, nil
}

// PeerIDFromNodeID reconstructs the PeerID for id. Authenticates nothing:
// a made-up NodeID yields a well-formed PeerID that fails at dial time
func PeerIDFromNodeID(id NodeID) (peer.ID, error) {
	pk, err := crypto.UnmarshalEd25519PublicKey(id[:])
	if err != nil {
		return "", fmt.Errorf("parsing node id as ed25519 key: %w", err)
	}
	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return "", fmt.Errorf("deriving peer id from node id: %w", err)
	}
	return pid, nil
}
