package sphinx

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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

// attachExchange starts a KeyExchange with the given record TTL on h
func attachExchange(t *testing.T, h host.Host, ttl time.Duration) (*KeyExchange, *KeyStore) {
	t.Helper()
	idPriv := h.Peerstore().PrivKey(h.ID())
	km, err := NewKeyManager(idPriv, ttl)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	ks := NewKeyStore()
	kx := NewKeyExchange(h, km, ks)
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
	if err := kxA.RequestKeys(ctx, plain.ID()); err == nil {
		t.Fatal("RequestKeys to a peer without the protocol: got nil error")
	}
	if _, ok := storeA.Get(plain.ID()); ok {
		t.Error("store holds a record for a peer that never sent one")
	}
}

func TestKeyExchangeOversizedMessageRejected(t *testing.T) {
	hostA, _, storeA := newExchangeHost(t)
	plain := newTestHost(t)

	connect(t, plain, hostA)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := plain.NewStream(ctx, hostA.ID(), ProtocolKeyExchange)
	if err != nil {
		t.Fatalf("opening raw stream: %v", err)
	}
	defer s.Reset()
	_ = s.SetDeadline(time.Now().Add(5 * time.Second))

	// Announce a 1 GiB frame; the responder must reject it before allocating
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], 1<<30)
	if _, err := s.Write(hdr[:n]); err != nil {
		t.Fatalf("writing oversized length prefix: %v", err)
	}

	// The handler resets the stream, so the read fails instead of returning a
	// response frame
	if _, err := readFrame(s); err == nil {
		t.Fatal("responder answered an oversized frame instead of resetting")
	}
	if _, ok := storeA.Get(plain.ID()); ok {
		t.Error("store holds a record despite the rejected exchange")
	}
}

func TestKeyExchangeRecordForOtherPeerRejected(t *testing.T) {
	hostA, _, storeA := newExchangeHost(t)
	plain := newTestHost(t)

	// A record validly signed by a third identity: the signer binding in
	// ConsumeKeyRecord passes, but the stream remote check must fail
	otherPriv, otherPid := newIdentity(t)
	rec := NewKeyRecord(otherPid, newSphinxKey(t), time.Hour)
	raw := sealToBytes(t, rec, otherPriv)

	connect(t, plain, hostA)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := plain.NewStream(ctx, hostA.ID(), ProtocolKeyExchange)
	if err != nil {
		t.Fatalf("opening raw stream: %v", err)
	}
	defer s.Reset()
	_ = s.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeFrame(s, raw); err != nil {
		t.Fatalf("writing frame: %v", err)
	}
	if _, err := readFrame(s); err == nil {
		t.Fatal("responder accepted a record for a different peer")
	}
	if _, ok := storeA.Get(otherPid); ok {
		t.Error("store holds the impersonated record")
	}
	if _, ok := storeA.Get(plain.ID()); ok {
		t.Error("store holds a record for the sending peer")
	}
}

func TestKeyExchangeNonEd25519PeerNotPooled(t *testing.T) {
	hostA, kxA, storeA := newExchangeHost(t)

	// A peer with a secp256k1 identity speaks the protocol and sends a
	// validly signed record, but must not enter the pool: its NodeID could
	// never be reconstructed into a PeerID by relays
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
	_, storeS := attachExchange(t, hostS, time.Hour)

	connect(t, hostA, hostS)

	// A full exchange succeeds; dropping the record is silent, not an error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kxA.RequestKeys(ctx, hostS.ID()); err != nil {
		t.Fatalf("RequestKeys to secp256k1 peer: %v", err)
	}

	if _, ok := storeA.Get(hostS.ID()); ok {
		t.Error("pool holds a record for a non-ed25519 peer")
	}
	// The filter is one-sided: the secp256k1 node may pool the ed25519 peer
	waitForRecord(t, storeS, hostA.ID())
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
	if err := kxA.RequestKeys(ctx, hostB.ID()); err != nil {
		t.Fatalf("repeat RequestKeys: %v", err)
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
