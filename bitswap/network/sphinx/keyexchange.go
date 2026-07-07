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
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var log = logging.Logger("bitswap/network/sphinx")

const ProtocolKeyExchange protocol.ID = "/sphinx/keys/1.0.0"

const (
	// a sealed record is a few hundred bytes; the cap blocks memory
	// exhaustion via lying length prefixes
	maxMessageSize = 4096

	// bounds one whole exchange on a stream
	exchangeTimeout = 10 * time.Second
)

// KeyExchange runs the symmetric key exchange: each side sends its sealed
// KeyRecord and validates the other's. Triggered for peers connected at
// construction, on every new connection, and periodically for records
// close to expiry
type KeyExchange struct {
	host  host.Host
	km    *KeyManager
	store *KeyStore

	// how close to expiry a record may get before the refresh loop
	// re-runs the exchange
	refreshWindow time.Duration

	notifiee *network.NotifyBundle
	done     chan struct{}

	mu       sync.Mutex
	closed   bool
	inflight map[peer.ID]struct{}
}

func NewKeyExchange(h host.Host, km *KeyManager, store *KeyStore) *KeyExchange {
	kx := &KeyExchange{
		host:          h,
		km:            km,
		store:         store,
		refreshWindow: km.ttl / 4,
		done:          make(chan struct{}),
		inflight:      make(map[peer.ID]struct{}),
	}
	kx.notifiee = &network.NotifyBundle{ConnectedF: kx.onConnected}
	h.SetStreamHandler(ProtocolKeyExchange, kx.handleStream)
	h.Network().Notify(kx.notifiee)
	// peers connected before construction never fire the notifiee
	for _, p := range h.Network().Peers() {
		kx.triggerExchange(p)
	}
	go kx.refreshLoop(refreshInterval(km.ttl))
	return kx
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

// Close stops the refresh loop, notifiee and handler; in-flight exchanges
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
	kx.host.RemoveStreamHandler(ProtocolKeyExchange)
	return nil
}

// RequestKeys runs one exchange with p as initiator
func (kx *KeyExchange) RequestKeys(ctx context.Context, p peer.ID) error {
	s, err := kx.host.NewStream(ctx, p, ProtocolKeyExchange)
	if err != nil {
		return fmt.Errorf("opening key exchange stream to %s: %w", p, err)
	}
	deadline := time.Now().Add(exchangeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := kx.runExchange(s, deadline, true); err != nil {
		_ = s.Reset()
		return err
	}
	return s.Close()
}

// responder side: receive and validate first, then answer
func (kx *KeyExchange) handleStream(s network.Stream) {
	if err := kx.runExchange(s, time.Now().Add(exchangeTimeout), false); err != nil {
		log.Debugw("inbound key exchange failed", "peer", s.Conn().RemotePeer(), "error", err)
		_ = s.Reset()
		return
	}
	_ = s.Close()
}

// initiator writes first, responder reads first
func (kx *KeyExchange) runExchange(s network.Stream, deadline time.Time, initiator bool) error {
	if err := s.SetDeadline(deadline); err != nil {
		return fmt.Errorf("setting stream deadline: %w", err)
	}
	send := func() error {
		own, err := kx.km.SealedRecord()
		if err != nil {
			return fmt.Errorf("sealing own key record: %w", err)
		}
		return writeFrame(s, own)
	}
	receive := func() error {
		raw, err := readFrame(s)
		if err != nil {
			return err
		}
		return kx.acceptRecord(raw, s.Conn().RemotePeer())
	}
	if initiator {
		if err := send(); err != nil {
			return err
		}
		return receive()
	}
	if err := receive(); err != nil {
		return err
	}
	return send()
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
		if err := kx.RequestKeys(ctx, p); err != nil {
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
