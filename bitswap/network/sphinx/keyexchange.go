package sphinx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var log = logging.Logger("bitswap/network/sphinx")

const ProtocolKeyExchange protocol.ID = "/sphinx/keys/2.0.0"

const (
	// a sealed record is a few hundred bytes; the cap blocks memory
	// exhaustion via lying length prefixes
	maxMessageSize = 4096

	// bounds one fetch or serve on a stream
	exchangeTimeout = 10 * time.Second
)

// KeyExchange is the pull side and, on relays, the serving side of the
// key protocol: every node fetches the sealed KeyRecord of peers it
// connects to, only relay-mode nodes answer with their own. Fetches are
// triggered for peers connected at construction, on every new connection,
// and periodically for records close to expiry
type KeyExchange struct {
	host  host.Host
	km    *KeyManager
	store *KeyStore

	// the exact protocol IDs whose presence makes ModeAuto serve
	kadProtos []protocol.ID

	// how close to expiry a record may get before the refresh loop
	// re-runs the fetch
	refreshWindow time.Duration

	notifiee *network.NotifyBundle
	done     chan struct{}

	// servingMu guards serving and the handler mount together, separate
	// from mu so the host call, which emits on the event bus, never runs
	// while mu is held
	servingMu sync.Mutex
	serving   bool
	// onServing, when set, is called under servingMu after every serving
	// change with the new state, so the relay transport can follow: a
	// serving node mounts the relay handler, a client removes it
	onServing func(bool)

	mu       sync.Mutex
	closed   bool
	inflight map[peer.ID]struct{}
}

func NewKeyExchange(h host.Host, km *KeyManager, store *KeyStore, mode Mode, kadProtos []protocol.ID) (*KeyExchange, error) {
	if len(kadProtos) == 0 {
		kadProtos = DefaultKadServerProtocols
	}
	// a node whose own identity cannot map to a NodeID never serves,
	// regardless of configured mode: its record could never enter any
	// pool, so serving would be pure waste, and paired with handleStream's
	// reverse trigger, two such nodes in relay mode would ping-pong
	// fetches at RTT pace for the connection's lifetime. Clamping here
	// kills the loop at the root, the counterpart's fetch simply fails
	// negotiation and no reverse trigger ever fires from this side
	if _, err := NodeIDFromPeer(km.PeerID()); err != nil {
		if mode != ModeClient {
			log.Warnw("clamping sphinx mode to client: own identity cannot serve as a hop",
				"peer", km.PeerID(), "configured_mode", mode, "error", err)
		}
		mode = ModeClient
	}
	kx := &KeyExchange{
		host:          h,
		km:            km,
		store:         store,
		kadProtos:     kadProtos,
		refreshWindow: km.ttl / 4,
		done:          make(chan struct{}),
		inflight:      make(map[peer.ID]struct{}),
	}
	switch mode {
	case ModeRelay:
		kx.setServing(true)
	case ModeClient:
		// never serves; nothing to mount
	case ModeAuto:
		sub, err := h.EventBus().Subscribe(new(event.EvtLocalProtocolsUpdated))
		if err != nil {
			return nil, fmt.Errorf("subscribing to protocol updates: %w", err)
		}
		// subscribe before the initial scan so no promotion between scan
		// and subscription is lost; a duplicate rescan is idempotent
		kx.setServing(kadServerMounted(h, kx.kadProtos))
		go kx.pumpProtocolEvents(sub, kx.watchServing())
	default:
		return nil, fmt.Errorf("invalid sphinx mode %d", mode)
	}
	kx.notifiee = &network.NotifyBundle{ConnectedF: kx.onConnected}
	h.Network().Notify(kx.notifiee)
	// peers connected before construction never fire the notifiee
	for _, p := range h.Network().Peers() {
		kx.triggerExchange(p)
	}
	go kx.refreshLoop(refreshInterval(km.ttl))
	return kx, nil
}

// must undercut the refresh window (ttl/4) so every expiring record is
// seen in time
func refreshInterval(ttl time.Duration) time.Duration {
	iv := ttl / 8
	if iv <= 0 {
		iv = time.Millisecond
	}
	return iv
}

// setServing mounts or removes the serving handler. Single writer after
// construction; lock order servingMu before mu, and Close removes the
// handler under servingMu too, so a mount can never survive Close
func (kx *KeyExchange) setServing(on bool) {
	kx.servingMu.Lock()
	defer kx.servingMu.Unlock()
	kx.mu.Lock()
	closed := kx.closed
	kx.mu.Unlock()
	if closed || on == kx.serving {
		return
	}
	kx.serving = on
	if on {
		kx.host.SetStreamHandler(ProtocolKeyExchange, kx.handleStream)
	} else {
		kx.host.RemoveStreamHandler(ProtocolKeyExchange)
	}
	if kx.onServing != nil {
		kx.onServing(on)
	}
}

// Serving reports whether the node currently answers key fetches
func (kx *KeyExchange) Serving() bool {
	kx.servingMu.Lock()
	defer kx.servingMu.Unlock()
	return kx.serving
}

// SetServingCallback registers f, called with the serving state on every
// change from here on. NewService uses it to mount and unmount the relay
// handler alongside the key handler. The caller runs the initial sync
// itself: the construction-time state predates this call, and a static
// client never triggers a change to report
func (kx *KeyExchange) SetServingCallback(f func(bool)) {
	kx.servingMu.Lock()
	kx.onServing = f
	kx.servingMu.Unlock()
}

// pumpProtocolEvents drains the subscription and coalesces into a 1-slot
// signal. Two stages on purpose: the go-libp2p event bus delivery blocks
// on full subscriber buffers, and setServing itself emits a protocol
// event when it mounts or removes the handler. A consumer that emits
// could deadlock against its own full buffer; the pump never blocks, so
// the bus never backs up on this subscription
func (kx *KeyExchange) pumpProtocolEvents(sub event.Subscription, recheck chan<- struct{}) {
	defer sub.Close()
	for {
		select {
		case <-kx.done:
			return
		case _, ok := <-sub.Out():
			if !ok {
				return
			}
			select {
			case recheck <- struct{}{}:
			default:
			}
		}
	}
}

// watchServing starts the applying goroutine and returns its signal
// channel. Every signal triggers a full rescan of the mounted protocols
// instead of interpreting event deltas; rescans are idempotent, so the
// event fired by our own mount converges after one extra pass. The guard
// against a mount->emit->rescan busy-loop is setServing's on == kx.serving
// early-return, not the rescan itself
func (kx *KeyExchange) watchServing() chan struct{} {
	recheck := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-kx.done:
				return
			case <-recheck:
				kx.setServing(kadServerMounted(kx.host, kx.kadProtos))
			}
		}
	}()
	return recheck
}

// Close stops the refresh loop, notifiee and handler; in-flight fetches
// may finish
func (kx *KeyExchange) Close() error {
	kx.mu.Lock()
	if kx.closed {
		kx.mu.Unlock()
		return nil
	}
	kx.closed = true
	kx.mu.Unlock()

	close(kx.done)
	kx.host.Network().StopNotify(kx.notifiee)
	kx.servingMu.Lock()
	kx.serving = false
	kx.host.RemoveStreamHandler(ProtocolKeyExchange)
	kx.servingMu.Unlock()
	return nil
}

// FetchKeys pulls p's key record over one stream. Peers that do not
// serve the protocol fail the negotiation, which is the capability
// filter: clients and vanilla IPFS nodes are skipped the same way
func (kx *KeyExchange) FetchKeys(ctx context.Context, p peer.ID) error {
	s, err := kx.host.NewStream(ctx, p, ProtocolKeyExchange)
	if err != nil {
		return fmt.Errorf("opening key fetch stream to %s: %w", p, err)
	}
	deadline := time.Now().Add(exchangeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := kx.fetch(s, deadline); err != nil {
		_ = s.Reset()
		return err
	}
	return s.Close()
}

func (kx *KeyExchange) fetch(s network.Stream, deadline time.Time) error {
	if err := s.SetDeadline(deadline); err != nil {
		return fmt.Errorf("setting stream deadline: %w", err)
	}
	raw, err := readFrame(s)
	if err != nil {
		return err
	}
	return kx.acceptRecord(raw, s.Conn().RemotePeer())
}

// the serving side: one frame with the own sealed record, no read
func (kx *KeyExchange) handleStream(s network.Stream) {
	if err := kx.serveKey(s); err != nil {
		log.Debugw("serving key record failed", "peer", s.Conn().RemotePeer(), "error", err)
		_ = s.Reset()
		return
	}
	_ = s.Close()
	// a fetcher is a sphinx node; opportunistically pull its record so
	// two relays coming up on an existing connection fill both pools
	// without waiting for the refresh tick. Clients fail the negotiation
	// as usual
	kx.triggerExchange(s.Conn().RemotePeer())
}

func (kx *KeyExchange) serveKey(s network.Stream) error {
	if err := s.SetDeadline(time.Now().Add(exchangeTimeout)); err != nil {
		return fmt.Errorf("setting stream deadline: %w", err)
	}
	own, err := kx.km.SealedRecord()
	if err != nil {
		return fmt.Errorf("sealing own key record: %w", err)
	}
	return writeFrame(s, own)
}

// acceptRecord validates and stores raw. A peer may only advertise its own
// key. Only Ed25519 peers enter the pool (a hop's NodeID must map back to
// a PeerID); non-Ed25519 and stale records are dropped silently
func (kx *KeyExchange) acceptRecord(raw []byte, remote peer.ID) error {
	rec, err := ConsumeKeyRecord(raw)
	if err != nil {
		return fmt.Errorf("invalid key record: %w", err)
	}
	if rec.PeerID != remote {
		return fmt.Errorf("record advertises peer %s, stream remote is %s", rec.PeerID, remote)
	}
	if _, err := NodeIDFromPeer(rec.PeerID); err != nil {
		log.Debugw("ignoring key record from non-ed25519 peer", "peer", remote, "error", err)
		return nil
	}
	if err := kx.store.Put(rec); err != nil {
		if errors.Is(err, ErrStaleRecord) {
			log.Debugw("ignoring stale key record", "peer", remote, "seq", rec.Seq)
			return nil
		}
		return fmt.Errorf("storing key record: %w", err)
	}
	return nil
}

func (kx *KeyExchange) onConnected(_ network.Network, c network.Conn) {
	kx.triggerExchange(c.RemotePeer())
}

// keeps peers on long-lived connections from dropping out of the pool
// when their record's TTL passes
func (kx *KeyExchange) refreshLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-kx.done:
			return
		case <-ticker.C:
			for _, p := range kx.host.Network().Peers() {
				kx.triggerExchange(p)
			}
		}
	}
}

// non-blocking (runs on the swarm's notification path); the exchange runs
// in its own goroutine, at most one per peer
func (kx *KeyExchange) triggerExchange(p peer.ID) {
	if !kx.needsExchange(p) {
		return
	}
	if !kx.tryBegin(p) {
		return
	}
	go func() {
		defer kx.finish(p)
		ctx, cancel := context.WithTimeout(context.Background(), exchangeTimeout)
		defer cancel()
		if err := kx.FetchKeys(ctx, p); err != nil {
			// peers without the protocol are skipped; negotiation failure
			// is the capability filter
			log.Debugw("triggered key exchange skipped", "peer", p, "error", err)
		}
	}()
}

// no live record, or the held one expires within the refresh window
func (kx *KeyExchange) needsExchange(p peer.ID) bool {
	info, ok := kx.store.Get(p)
	if !ok {
		return true
	}
	return time.Until(info.Record.Expiry) < kx.refreshWindow
}

func (kx *KeyExchange) tryBegin(p peer.ID) bool {
	kx.mu.Lock()
	defer kx.mu.Unlock()
	if kx.closed {
		return false
	}
	if _, ok := kx.inflight[p]; ok {
		return false
	}
	kx.inflight[p] = struct{}{}
	return true
}

func (kx *KeyExchange) finish(p peer.ID) {
	kx.mu.Lock()
	delete(kx.inflight, p)
	kx.mu.Unlock()
}

// uvarint length prefix + payload
func writeFrame(w io.Writer, msg []byte) error {
	if len(msg) > maxMessageSize {
		return fmt.Errorf("frame of %d bytes exceeds limit %d", len(msg), maxMessageSize)
	}
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(msg)))
	if _, err := w.Write(hdr[:n]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

// enforces maxMessageSize before allocating
func readFrame(r io.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(oneByteReader{r})
	if err != nil {
		return nil, fmt.Errorf("reading frame length: %w", err)
	}
	if n > maxMessageSize {
		return nil, fmt.Errorf("frame of %d bytes exceeds limit %d", n, maxMessageSize)
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, fmt.Errorf("reading frame body: %w", err)
	}
	return msg, nil
}

// io.ByteReader without read-ahead, so the frame body stays in the stream
type oneByteReader struct{ r io.Reader }

func (b oneByteReader) ReadByte() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(b.r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}
