package sphinx

import (
	"context"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// newTestHost returns a libp2p host listening on TCP loopback
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	idPriv, _ := newIdentity(t)
	h, err := libp2p.New(
		libp2p.Identity(idPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatalf("creating host: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// attachExchange starts a relay-mode KeyExchange with the given record
// TTL on h
func attachExchange(t *testing.T, h host.Host, ttl time.Duration) (*KeyExchange, *KeyStore) {
	t.Helper()
	return attachExchangeMode(t, h, ttl, ModeRelay)
}

// attachExchangeMode is attachExchange with an explicit serving mode
func attachExchangeMode(t *testing.T, h host.Host, ttl time.Duration, mode Mode) (*KeyExchange, *KeyStore) {
	t.Helper()
	idPriv := h.Peerstore().PrivKey(h.ID())
	km, err := NewKeyManager(idPriv, ttl)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	ks := NewKeyStore()
	kx, err := NewKeyExchange(h, km, ks, mode, nil)
	if err != nil {
		t.Fatalf("NewKeyExchange: %v", err)
	}
	t.Cleanup(func() { kx.Close() })
	return kx, ks
}

// newExchangeHost returns a host with a running KeyExchange and its KeyStore
func newExchangeHost(t *testing.T) (host.Host, *KeyExchange, *KeyStore) {
	t.Helper()
	h := newTestHost(t)
	kx, ks := attachExchange(t, h, time.Hour)
	return h, kx, ks
}

// connect dials from a to b and fails the test on error
func connect(t *testing.T, a, b host.Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatalf("connecting %s to %s: %v", a.ID(), b.ID(), err)
	}
}

// waitForRecord polls ks until it holds a live record for p
func waitForRecord(t *testing.T, ks *KeyStore, p peer.ID) KeyInfo {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for key record of %s", p)
		case <-tick.C:
			if info, ok := ks.Get(p); ok {
				return info
			}
		}
	}
}

func TestKeyExchangeOnConnect(t *testing.T) {
	hostA, _, storeA := newExchangeHost(t)
	hostB, _, storeB := newExchangeHost(t)

	connect(t, hostA, hostB)

	infoB := waitForRecord(t, storeA, hostB.ID())
	infoA := waitForRecord(t, storeB, hostA.ID())

	if infoB.Record.PeerID != hostB.ID() {
		t.Errorf("store A holds record for %s, want %s", infoB.Record.PeerID, hostB.ID())
	}
	if infoA.Record.PeerID != hostA.ID() {
		t.Errorf("store B holds record for %s, want %s", infoA.Record.PeerID, hostA.ID())
	}
}

func TestKeyExchangeExistingConnectionsAtStartup(t *testing.T) {
	hostA := newTestHost(t)
	hostB := newTestHost(t)

	// The connection exists before either KeyExchange, so no Connected
	// notification ever fires for them; the startup scan must cover this
	connect(t, hostA, hostB)

	_, storeA := attachExchange(t, hostA, time.Hour)
	_, storeB := attachExchange(t, hostB, time.Hour)

	waitForRecord(t, storeA, hostB.ID())
	waitForRecord(t, storeB, hostA.ID())
}

func TestKeyExchangeRefreshesExpiringRecords(t *testing.T) {
	// Short TTL so the refresh window (ttl/4) is reached quickly
	const ttl = 2 * time.Second
	hostA := newTestHost(t)
	hostB := newTestHost(t)
	_, storeA := attachExchange(t, hostA, ttl)
	attachExchange(t, hostB, ttl)

	connect(t, hostA, hostB)
	first := waitForRecord(t, storeA, hostB.ID())

	// Without any new connection event, the record must be replaced by a
	// fresher one (higher Seq) before it expires and drops out of the pool
	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("record was never refreshed over the long-lived connection")
		case <-tick.C:
			info, ok := storeA.Get(hostB.ID())
			if !ok {
				t.Fatal("record dropped out of the pool before being refreshed")
			}
			if info.Record.Seq > first.Record.Seq {
				return
			}
		}
	}
}

func TestKeyExchangePeerWithoutProtocol(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)
	plain := newTestHost(t) // no KeyExchange, no handler

	connect(t, hostA, plain)

	// An explicit exchange fails with a negotiation error and leaves no record
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, plain.ID()); err == nil {
		t.Fatal("FetchKeys to a peer without the protocol: got nil error")
	}
	if _, ok := storeA.Get(plain.ID()); ok {
		t.Error("store holds a record for a peer that never sent one")
	}
}

func TestKeyExchangeOversizedRecordRejected(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)
	evil := newTestHost(t)

	// a fake server announcing a 1 GiB frame; the fetcher must reject the
	// length before allocating
	evil.SetStreamHandler(ProtocolKeyExchange, func(s network.Stream) {
		defer s.Close()
		var hdr [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(hdr[:], 1<<30)
		_, _ = s.Write(hdr[:n])
	})

	connect(t, hostA, evil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, evil.ID()); err == nil {
		t.Fatal("fetcher accepted an oversized frame")
	}
	if _, ok := storeA.Get(evil.ID()); ok {
		t.Error("store holds a record despite the rejected fetch")
	}
}

func TestKeyExchangeRecordForOtherPeerRejected(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)
	evil := newTestHost(t)

	// a record validly signed by a third identity: the signer binding in
	// ConsumeKeyRecord passes, but the stream remote check must fail
	otherPriv, otherPid := newIdentity(t)
	rec := NewKeyRecord(otherPid, newSphinxKey(t), time.Hour)
	raw := sealToBytes(t, rec, otherPriv)
	evil.SetStreamHandler(ProtocolKeyExchange, func(s network.Stream) {
		defer s.Close()
		_ = writeFrame(s, raw)
	})

	connect(t, hostA, evil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, evil.ID()); err == nil {
		t.Fatal("fetcher accepted a record for a different peer")
	}
	if _, ok := storeA.Get(otherPid); ok {
		t.Error("store holds the impersonated record")
	}
	if _, ok := storeA.Get(evil.ID()); ok {
		t.Error("store holds a record for the serving peer")
	}
}

func TestKeyExchangeNonEd25519PeerNotPooled(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)

	// A peer with a secp256k1 identity can never enter any pool: its
	// NodeID could never be reconstructed into a PeerID by relays.
	// NewKeyExchange clamps such an identity to client mode by itself, so
	// even though this exchange is attached with ModeRelay it never mounts
	// the serving handler, and a fetch against it fails negotiation
	// instead of completing with a silently dropped record
	secpPriv, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generating secp256k1 identity: %v", err)
	}
	hostS, err := libp2p.New(
		libp2p.Identity(secpPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatalf("creating secp256k1 host: %v", err)
	}
	t.Cleanup(func() { hostS.Close() })
	kxS, storeS := attachExchange(t, hostS, time.Hour)

	if kxS.Serving() {
		t.Error("secp256k1 exchange serves despite ModeRelay: identity cannot map to a NodeID")
	}

	connect(t, hostA, hostS)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, hostS.ID()); err == nil {
		t.Fatal("FetchKeys to a non-ed25519 identity: got nil error, want a negotiation failure")
	}

	if _, ok := storeA.Get(hostS.ID()); ok {
		t.Error("pool holds a record for a non-ed25519 peer")
	}
	// The filter is one-sided: the secp256k1 node may still pool the ed25519 peer
	waitForRecord(t, storeS, hostA.ID())
}

func TestKeyExchangeNonEd25519RecordDroppedOnWire(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)

	// The mode clamp keeps a non-ed25519 KeyExchange from ever serving, so
	// acceptRecord's silent-drop branch needs a raw handler to be reached
	// over the wire: a secp256k1 host mounts the protocol directly and
	// serves its own validly sealed record. The fetch must succeed (the
	// drop is silent, not an error) and the record must stay out of the pool
	secpPriv, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generating secp256k1 identity: %v", err)
	}
	hostS, err := libp2p.New(
		libp2p.Identity(secpPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatalf("creating secp256k1 host: %v", err)
	}
	t.Cleanup(func() { hostS.Close() })
	rec := NewKeyRecord(hostS.ID(), newSphinxKey(t), time.Hour)
	raw := sealToBytes(t, rec, secpPriv)
	hostS.SetStreamHandler(ProtocolKeyExchange, func(s network.Stream) {
		defer s.Close()
		_ = writeFrame(s, raw)
	})

	connect(t, hostA, hostS)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, hostS.ID()); err != nil {
		t.Fatalf("FetchKeys serving a non-ed25519 record: %v", err)
	}
	if _, ok := storeA.Get(hostS.ID()); ok {
		t.Error("pool holds a record for a non-ed25519 peer")
	}
}

func TestKeyExchangeRepeatIsStaleNotError(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)
	hostB, _, _ := newExchangeHost(t)

	connect(t, hostA, hostB)
	first := waitForRecord(t, storeA, hostB.ID())

	// A second exchange re-sends the cached records (same Seq). The store
	// rejects them as stale, which the exchange must not treat as failure
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, hostB.ID()); err != nil {
		t.Fatalf("repeat FetchKeys: %v", err)
	}
	info, ok := storeA.Get(hostB.ID())
	if !ok {
		t.Fatal("record disappeared after repeat exchange")
	}
	if info.Record.Seq != first.Record.Seq {
		t.Errorf("stored Seq changed from %d to %d on stale re-exchange",
			first.Record.Seq, info.Record.Seq)
	}
}

func TestKeyExchangeClientServesNothing(t *testing.T) {
	hostR, kxR, storeR := newExchangeHost(t)
	hostC := newTestHost(t)
	_, storeC := attachExchangeMode(t, hostC, time.Hour, ModeClient)

	connect(t, hostC, hostR)

	// the client pools the relay; that proves the exchange machinery ran
	waitForRecord(t, storeC, hostR.ID())

	if slices.Contains(hostC.Mux().Protocols(), ProtocolKeyExchange) {
		t.Error("client mounts the key exchange handler")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := kxR.FetchKeys(ctx, hostC.ID()); err == nil {
		t.Fatal("fetch from a client succeeded")
	}
	if _, ok := storeR.Get(hostC.ID()); ok {
		t.Error("relay pool holds the client key")
	}
}

func TestKeyExchangeAutoFollowsKadProtocol(t *testing.T) {
	kad := DefaultKadServerProtocols[0]
	hostX := newTestHost(t)
	kxX, _ := attachExchangeMode(t, hostX, time.Hour, ModeAuto)
	if kxX.Serving() {
		t.Fatal("auto mode serves without a kad server protocol")
	}

	hostA, kxA, storeA := newExchangeHost(t)
	connect(t, hostA, hostX)

	// promotion: mounting the kad server protocol must mount key serving
	hostX.SetStreamHandler(kad, func(s network.Stream) { _ = s.Reset() })
	waitFor(t, 10*time.Second, "auto promotion to serving", kxX.Serving)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.FetchKeys(ctx, hostX.ID()); err != nil {
		t.Fatalf("fetch after promotion: %v", err)
	}
	if _, ok := storeA.Get(hostX.ID()); !ok {
		t.Error("no record pooled after promotion")
	}

	// demotion: removing the kad protocol must withdraw serving
	hostX.RemoveStreamHandler(kad)
	waitFor(t, 10*time.Second, "auto demotion", func() bool { return !kxX.Serving() })
}

func TestKeyExchangeAutoIgnoresLANDHT(t *testing.T) {
	lan := protocol.ID("/ipfs/lan/kad/1.0.0")
	wan := DefaultKadServerProtocols[0]

	// Kubo's dual DHT mounts the LAN server protocol on every node; it
	// must count neither at construction nor on later rescans
	hostX := newTestHost(t)
	hostX.SetStreamHandler(lan, func(s network.Stream) { _ = s.Reset() })
	kxX, _ := attachExchangeMode(t, hostX, time.Hour, ModeAuto)
	if kxX.Serving() {
		t.Fatal("LAN kad protocol alone promoted the node")
	}

	// promote via the WAN protocol, then remove it again: the rescan on
	// removal sees only the LAN protocol left and must demote
	hostX.SetStreamHandler(wan, func(s network.Stream) { _ = s.Reset() })
	waitFor(t, 10*time.Second, "promotion via wan kad", kxX.Serving)
	hostX.RemoveStreamHandler(wan)
	waitFor(t, 10*time.Second, "demotion despite mounted lan kad", func() bool { return !kxX.Serving() })
}
