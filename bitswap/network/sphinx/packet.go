package sphinx

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/katzenpost/hpqc/rand"
	kpsphinx "github.com/katzenpost/katzenpost/core/sphinx"
	"github.com/libp2p/go-libp2p/core/peer"
)

// big-endian length prefix inside the padded payload
const payloadLenPrefix = 2

func newSphinx() (*kpsphinx.Sphinx, error) {
	sph, err := kpsphinx.FromGeometry(Geometry())
	if err != nil {
		return nil, fmt.Errorf("constructing sphinx from pinned geometry: %w", err)
	}
	return sph, nil
}

// NewForwardPacket builds the packet that travels over hops and delivers
// payload to the terminal hop, addressed to the handler named by recipient
func NewForwardPacket(hops []KeyInfo, recipient RecipientID, payload []byte) ([]byte, error) {
	sph, err := newSphinx()
	if err != nil {
		return nil, err
	}
	g := sph.Geometry()
	path, err := ForwardPath(hops, recipient)
	if err != nil {
		return nil, fmt.Errorf("building forward path: %w", err)
	}
	padded, err := padPayload(payload, g.ForwardPayloadLength)
	if err != nil {
		return nil, err
	}
	pkt, err := sph.NewPacket(rand.Reader, path, padded)
	if err != nil {
		return nil, fmt.Errorf("building forward packet: %w", err)
	}
	if len(pkt) != g.PacketLength {
		return nil, fmt.Errorf("built packet is %d bytes, want %d", len(pkt), g.PacketLength)
	}
	return pkt, nil
}

// NewSURB builds a single-use reply block over hops; the terminal hop must
// be the requesting node. Storing id -> keys is the caller's job
func NewSURB(hops []KeyInfo) (surb []byte, id SURBID, keys []byte, err error) {
	sph, err := newSphinx()
	if err != nil {
		return nil, id, nil, err
	}
	if _, err := crand.Read(id[:]); err != nil {
		return nil, id, nil, fmt.Errorf("generating surb id: %w", err)
	}
	path, err := ReturnPath(hops, id)
	if err != nil {
		return nil, id, nil, fmt.Errorf("building return path: %w", err)
	}
	surb, keys, err = sph.NewSURB(rand.Reader, path)
	if err != nil {
		return nil, id, nil, fmt.Errorf("building surb: %w", err)
	}
	if len(surb) != sph.Geometry().SURBLength {
		return nil, id, nil, fmt.Errorf("built surb is %d bytes, want %d", len(surb), sph.Geometry().SURBLength)
	}
	return surb, id, keys, nil
}

// NewReplyFromSURB builds the reply packet for a received SURB and
// resolves the first return hop (the only hop the responder learns).
// katzenpost does not check the packet length here, so we do
func NewReplyFromSURB(surb, payload []byte) ([]byte, peer.ID, error) {
	sph, err := newSphinx()
	if err != nil {
		return nil, "", err
	}
	g := sph.Geometry()
	padded, err := padPayload(payload, g.ForwardPayloadLength)
	if err != nil {
		return nil, "", err
	}
	pkt, firstHop, err := sph.NewPacketFromSURB(surb, padded)
	if err != nil {
		return nil, "", fmt.Errorf("building reply from surb: %w", err)
	}
	if len(pkt) != g.PacketLength {
		return nil, "", fmt.Errorf("built reply packet is %d bytes, want %d", len(pkt), g.PacketLength)
	}
	next, err := PeerIDFromNodeID(*firstHop)
	if err != nil {
		return nil, "", fmt.Errorf("resolving first return hop: %w", err)
	}
	return pkt, next, nil
}

// padPayload frames payload to exactly size bytes: length prefix, payload,
// zero padding. katzenpost requires exactly ForwardPayloadLength, and the
// uniform size keeps lengths from leaking content. The prefix puts
// capacity (2046 B) 2 B under the 2048 B user budget; the geometry pin
// test proves every worst-case message fits the wire
func padPayload(payload []byte, size int) ([]byte, error) {
	if len(payload) > size-payloadLenPrefix {
		return nil, fmt.Errorf("payload of %d bytes exceeds capacity %d", len(payload), size-payloadLenPrefix)
	}
	padded := make([]byte, size)
	binary.BigEndian.PutUint16(padded, uint16(len(payload)))
	copy(padded[payloadLenPrefix:], payload)
	return padded, nil
}

// returned slice aliases padded
func unpadPayload(padded []byte) ([]byte, error) {
	if len(padded) < payloadLenPrefix {
		return nil, fmt.Errorf("padded payload of %d bytes is shorter than its length prefix", len(padded))
	}
	n := int(binary.BigEndian.Uint16(padded))
	if n > len(padded)-payloadLenPrefix {
		return nil, fmt.Errorf("length prefix %d exceeds padded payload capacity %d", n, len(padded)-payloadLenPrefix)
	}
	return padded[payloadLenPrefix : payloadLenPrefix+n], nil
}
