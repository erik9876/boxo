package sphinx

import (
	"errors"
	"fmt"

	kpsphinx "github.com/katzenpost/katzenpost/core/sphinx"
	"github.com/katzenpost/katzenpost/core/sphinx/commands"
	"github.com/katzenpost/katzenpost/core/sphinx/geo"
	"github.com/libp2p/go-libp2p/core/peer"
)

var ErrReplay = errors.New("replayed sphinx packet")

// SURB already used, or the reply was never ours
var ErrUnknownSURB = errors.New("no keys stored for surb reply")

// DeliveryHandler receives payloads that exit the Sphinx layer at this
// node; both methods are called synchronously from ProcessPacket
type DeliveryHandler interface {
	// recipient is the packet's demux field; the handler decides whether
	// it serves it
	HandleDelivery(recipient RecipientID, payload []byte)

	// decrypted payload of a reply through a SURB this node created
	HandleSURBReply(id SURBID, payload []byte)
}

// Forward is a processed packet that travels on
type Forward struct {
	Packet []byte
	Next   peer.ID
}

// Relay peels one encryption layer off inbound packets and forwards,
// delivers locally, or resolves a SURB reply against the SURBStore
type Relay struct {
	sph     *kpsphinx.Sphinx
	geo     *geo.Geometry
	km      *KeyManager
	replay  *ReplayFilter
	surbs   *SURBStore
	handler DeliveryHandler
}

// the replay filter is created here because its scope is exactly the
// KeyManager's key epoch
func NewRelay(km *KeyManager, surbs *SURBStore, handler DeliveryHandler) (*Relay, error) {
	if km == nil || surbs == nil || handler == nil {
		return nil, fmt.Errorf("relay needs a key manager, a surb store and a delivery handler")
	}
	sph, err := newSphinx()
	if err != nil {
		return nil, err
	}
	return &Relay{
		sph:     sph,
		geo:     sph.Geometry(),
		km:      km,
		replay:  NewReplayFilter(),
		surbs:   surbs,
		handler: handler,
	}, nil
}

// ProcessPacket peels one layer off pkt, mutating it in place (caller must
// not reuse the buffer). Returns a *Forward when the packet travels on,
// (nil, nil) after local delivery or SURB reply, an error when the packet
// must be dropped
func (r *Relay) ProcessPacket(pkt []byte) (*Forward, error) {
	if len(pkt) != r.geo.PacketLength {
		return nil, fmt.Errorf("packet is %d bytes, want %d", len(pkt), r.geo.PacketLength)
	}
	payload, tag, cmds, err := r.safeUnwrap(pkt)
	if err != nil {
		return nil, fmt.Errorf("unwrapping packet: %w", err)
	}
	if r.replay.TestAndSet(tag) {
		return nil, ErrReplay
	}

	var nextNode *commands.NextNodeHop
	var surbReply *commands.SURBReply
	var recipient *commands.Recipient
	for _, cmd := range cmds {
		switch c := cmd.(type) {
		case *commands.NextNodeHop:
			nextNode = c
		case *commands.SURBReply:
			surbReply = c
		case *commands.Recipient:
			recipient = c
		}
	}

	switch {
	case nextNode != nil:
		// Unwrap transformed pkt in place into the next hop's packet
		next, err := PeerIDFromNodeID(nextNode.ID)
		if err != nil {
			return nil, fmt.Errorf("resolving next hop: %w", err)
		}
		return &Forward{Packet: pkt, Next: next}, nil

	case surbReply != nil:
		// duplicate replies share header and replay tag, so the filter
		// above already rejected them; that makes the non-atomic
		// Get/Delete safe. Get hands out a key copy because
		// DecryptSURBPayload zeroes what it is given
		keys, ok := r.surbs.Get(surbReply.ID)
		if !ok {
			return nil, fmt.Errorf("surb id %x: %w", surbReply.ID, ErrUnknownSURB)
		}
		padded, err := r.sph.DecryptSURBPayload(payload, keys)
		if err != nil {
			return nil, fmt.Errorf("decrypting surb reply %x: %w", surbReply.ID, err)
		}
		plain, err := unpadPayload(padded)
		if err != nil {
			return nil, fmt.Errorf("unpadding surb reply %x: %w", surbReply.ID, err)
		}
		r.surbs.Delete(surbReply.ID)
		r.handler.HandleSURBReply(surbReply.ID, plain)
		return nil, nil

	case recipient != nil:
		// terminal delivery; payload tag already verified by Unwrap
		plain, err := unpadPayload(payload)
		if err != nil {
			return nil, fmt.Errorf("unpadding delivered payload: %w", err)
		}
		r.handler.HandleDelivery(recipient.ID, plain)
		return nil, nil
	}

	return nil, fmt.Errorf("packet carries no routing command")
}

// safeUnwrap converts a crypto-layer panic into an error: hpqc's X25519
// DeriveSecret panics on a low-order group element, which an attacker can
// put into a header; a relay must drop that packet, not crash (DoS).
// Confidentiality is unaffected, the header MAC fails without the real hop
// key. Logged at Warn so a real katzenpost bug stays visible
func (r *Relay) safeUnwrap(pkt []byte) (payload, tag []byte, cmds []commands.RoutingCommand, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			payload, tag, cmds = nil, nil, nil
			err = fmt.Errorf("unwrap panicked on malformed packet: %v", rec)
			log.Warnw("recovered from panic while unwrapping sphinx packet", "error", rec)
		}
	}()
	return r.sph.Unwrap(r.km.PrivateKey(), pkt)
}
