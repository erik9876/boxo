package sphinx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const ProtocolRelay protocol.ID = "/sphinx/relay/1.0.0"

// bounds sending or receiving one packet on a stream
const relayTimeout = 10 * time.Second

// ErrUndeliverable: dial or write to the next hop failed. Normal outcome,
// the pool guarantees prior contact, not a live connection; callers treat
// it as a drop
var ErrUndeliverable = errors.New("packet could not be delivered to next hop")

var ErrTransportClosed = errors.New("sphinx transport is closed")

// PeerFinder resolves a peer's addresses over the network; the DHT's
// FindPeer satisfies it (routing.PeerRouting). Note that the stock kad-dht
// FindPeer answers from local state for connected peers without a network
// walk; callers who need a lookup on the wire every time must wrap it
type PeerFinder interface {
	FindPeer(ctx context.Context, p peer.ID) (peer.AddrInfo, error)
}

// Transport wires a Relay to a libp2p host. One packet per stream,
// fire-and-forget; replies come back later on a fresh stream the return
// path opens. Serving nodes run the inbound handler, which is also how an
// initiator (always a relay-set member) receives its own SURB replies; a
// client-mode node keeps it unmounted (setServing).
//
// With a finder, every send is preceded by one FindPeer for the next hop,
// known or not. All roles send through here, so initiator, relay and proxy
// hops behave identically: a hop that had to resolve its successor is
// indistinguishable from one that already knew it
type Transport struct {
	host      host.Host
	relay     *Relay
	finder    PeerFinder
	packetLen int

	metrics TransportMetrics

	mu     sync.Mutex
	closed bool
	// tracks the relay handler so setServing is idempotent; NewTransport
	// starts it mounted, NewService then drives it from the serving state
	mounted bool
}

// NewTransport registers the relay handler on h. finder may be nil: sends
// then dial straight from the peerstore
func NewTransport(h host.Host, relay *Relay, finder PeerFinder) *Transport {
	t := &Transport{
		host:      h,
		relay:     relay,
		finder:    finder,
		packetLen: relay.geo.PacketLength,
		mounted:   true,
	}
	h.SetStreamHandler(ProtocolRelay, t.handleStream)
	return t
}

// setServing mounts or removes the relay handler to follow the node's
// serving state, so a client-mode node advertises no sphinx protocol at
// all. Idempotent and a no-op after Close; NewService drives it from the
// key exchange
func (t *Transport) setServing(on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || on == t.mounted {
		return
	}
	t.mounted = on
	if on {
		t.host.SetStreamHandler(ProtocolRelay, t.handleStream)
	} else {
		t.host.RemoveStreamHandler(ProtocolRelay)
	}
}

func (t *Transport) Metrics() TransportMetricsSnapshot {
	return t.metrics.Snapshot()
}

// Close deregisters the handler and stops new sends; in-flight forwards
// may finish
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mounted = false
	t.mu.Unlock()

	t.host.RemoveStreamHandler(ProtocolRelay)
	return nil
}

// SendPacket opens a stream to next and writes pkt (exactly PacketLength).
// A failed dial or write is a benign drop (ErrUndeliverable)
func (t *Transport) SendPacket(ctx context.Context, next peer.ID, pkt []byte) error {
	if len(pkt) != t.packetLen {
		return fmt.Errorf("refusing to send packet of %d bytes, want %d", len(pkt), t.packetLen)
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrTransportClosed
	}

	t.lookupNextHop(ctx, next)

	s, err := t.host.NewStream(ctx, next, ProtocolRelay)
	if err != nil {
		log.Debugw("relay dial failed, dropping packet", "peer", next, "error", err)
		return fmt.Errorf("dialing %s: %w", next, ErrUndeliverable)
	}

	deadline := time.Now().Add(relayTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		_ = s.Reset()
		return fmt.Errorf("setting relay stream deadline: %w", err)
	}
	if _, err := s.Write(pkt); err != nil {
		_ = s.Reset()
		log.Debugw("relay write failed, dropping packet", "peer", next, "error", err)
		return fmt.Errorf("writing packet to %s: %w", next, ErrUndeliverable)
	}
	// Close flushes and sends FIN; no reply expected on this stream
	return s.Close()
}

// lookupNextHop runs the pre-send FindPeer, unconditionally: fresh
// addresses ride in for free and every hop pays the same lookup whether it
// knows the next one or not. Failure is fine, the dial decides
// deliverability
func (t *Transport) lookupNextHop(ctx context.Context, next peer.ID) {
	if t.finder == nil {
		return
	}
	lctx, cancel := context.WithTimeout(ctx, relayTimeout)
	defer cancel()
	t.metrics.Lookups.Add(1)
	ai, err := t.finder.FindPeer(lctx, next)
	if err != nil {
		t.metrics.LookupFailures.Add(1)
		log.Debugw("next-hop lookup failed", "peer", next, "error", err)
		return
	}
	if len(ai.Addrs) > 0 {
		t.host.Peerstore().AddAddrs(next, ai.Addrs, peerstore.TempAddrTTL)
	}
}

// handleStream reads exactly one packet, processes it, forwards if needed.
// Fixed packet size = trivial framing: io.ReadFull, no length prefix
func (t *Transport) handleStream(s network.Stream) {
	remote := s.Conn().RemotePeer()
	if err := s.SetReadDeadline(time.Now().Add(relayTimeout)); err != nil {
		_ = s.Reset()
		return
	}

	pkt := make([]byte, t.packetLen)
	if _, err := io.ReadFull(s, pkt); err != nil {
		log.Debugw("reading relay packet failed", "peer", remote, "error", err)
		_ = s.Reset()
		return
	}
	// discards anything written past PacketLength
	_ = s.Close()

	fwd, err := t.relay.ProcessPacket(pkt)
	if err != nil {
		// replays etc. are expected in a live network; delivery and SURB
		// replies were already handled inside ProcessPacket
		log.Debugw("processing relay packet failed", "peer", remote, "error", err)
		return
	}
	if fwd == nil {
		return
	}

	// forward on the handler goroutine: libp2p already runs one per
	// stream and the inbound stream is closed. Cost: one goroutine per
	// inbound packet, no backpressure; bounding that is left for later
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	_ = t.SendPacket(ctx, fwd.Next, fwd.Packet)
}
