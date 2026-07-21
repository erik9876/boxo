package sphinx

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	rpqm "github.com/ipfs/boxo/routing/providerquerymanager"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// newServiceCluster brings up n full Services on loopback hosts, every
// node with disc as its canned proxy discoverer, and connects every pair,
// so the key exchanges fill all pools. Callers wait on PoolStatus before
// starting jobs; the exchange runs asynchronously after connect
func newServiceCluster(t *testing.T, n int, disc ProviderDiscoverer, cfg ServiceConfig) ([]*Service, []host.Host) {
	t.Helper()
	svcs := make([]*Service, n)
	hosts := make([]host.Host, n)
	for i := range svcs {
		h := newTestHost(t)
		nodeCfg := cfg
		nodeCfg.Mode = ModeRelay
		nodeCfg.Proxy.Discoverer = disc
		svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, nodeCfg)
		if err != nil {
			t.Fatalf("NewService %d: %v", i, err)
		}
		t.Cleanup(func() { _ = svc.Close() })
		svcs[i], hosts[i] = svc, h
	}
	connectAll(t, hosts...)
	return svcs, hosts
}

// waitForPool blocks until svc's live pool covers one job's demand
func waitForPool(t *testing.T, svc *Service) {
	t.Helper()
	waitFor(t, 30*time.Second, "the relay pool to fill", func() bool {
		live, need := svc.PoolStatus()
		return live >= need
	})
}

// the reference wiring, end to end on loopback hosts: NewService per
// node, pools filled by the real key exchange, ProviderFinder wrapped in
// an explicit PQM (WithMaxTimeout from SuggestedFinderTimeout). Advertised
// providers are cluster hosts, so the PQM's connect-filter passes them
func TestServiceEndToEnd(t *testing.T) {
	cfg := ServiceConfig{Job: JobConfig{Timeout: 20 * time.Second}}
	need := jobSampleSize(DefaultBranchesPerJob, ReturnPathsPerJob)

	// Providers must be dialable for the ProviderQueryManager: use real
	// cluster members. The discoverer is set after cluster construction so
	// it can name them
	disc := &fakeDiscoverer{}
	svcs, hosts := newServiceCluster(t, need+1, disc, cfg)
	wantIDs := make([]peer.ID, 3)
	for i := range wantIDs {
		h := hosts[1+i]
		wantIDs[i] = h.ID()
		disc.providers = append(disc.providers, peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()[:1]})
	}

	initiator := svcs[0]
	waitForPool(t, initiator)

	finder := NewProviderFinder(initiator.Jobs())
	pqm, err := rpqm.New(hosts[0], finder,
		rpqm.WithMaxTimeout(SuggestedFinderTimeout(cfg.Job)),
		rpqm.WithMaxProviders(MaxProvidersPerReply))
	if err != nil {
		t.Fatalf("providerquerymanager.New: %v", err)
	}
	defer pqm.Close()

	got := collectProviders(t, pqm.FindProvidersAsync(context.Background(), testCID(t), 0), 60*time.Second)
	gotIDs := make([]peer.ID, len(got))
	for i, p := range got {
		gotIDs[i] = p.ID
	}
	for _, want := range wantIDs {
		if !slices.Contains(gotIDs, want) {
			t.Errorf("provider %s missing from the lookup result %v", want, gotIDs)
		}
	}
	if len(got) != len(wantIDs) {
		t.Errorf("lookup yielded %d providers, want %d", len(got), len(wantIDs))
	}

	if met := initiator.JobMetrics(); met.JobsSucceeded != 1 {
		t.Errorf("JobsSucceeded = %d, want 1", met.JobsSucceeded)
	}

	// Down again, twice: Close is idempotent on a service that saw real
	// traffic
	if err := initiator.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := initiator.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestServiceClientMode: a ModeClient service fills its pool from relays
// and never appears in theirs, but is refused when it tries to initiate:
// only relay-set members start jobs
func TestServiceClientMode(t *testing.T) {
	cfg := ServiceConfig{Job: JobConfig{Branches: 1, ReturnPaths: 1, Timeout: 20 * time.Second}}
	disc := &fakeDiscoverer{}
	svcs, hosts := newServiceCluster(t, jobSampleSize(1, 1), disc, cfg)

	hc := newTestHost(t)
	clientCfg := cfg
	clientCfg.Mode = ModeClient
	clientCfg.Proxy.Discoverer = disc
	clientSvc, err := NewService(hc, hc.Peerstore().PrivKey(hc.ID()), nil, clientCfg)
	if err != nil {
		t.Fatalf("NewService client: %v", err)
	}
	defer clientSvc.Close()
	for _, h := range hosts {
		connect(t, hc, h)
	}

	waitForPool(t, clientSvc)
	if got := clientSvc.Mode(); got != ModeClient {
		t.Errorf("client Mode() = %v, want ModeClient", got)
	}
	if got := svcs[0].Mode(); got != ModeRelay {
		t.Errorf("relay Mode() = %v, want ModeRelay", got)
	}
	// relays fetch from the client the moment it connects and fail the
	// negotiation; no record can ever exist for it
	for i, svc := range svcs {
		if _, ok := svc.pool.Get(hc.ID()); ok {
			t.Errorf("relay %d pools the client key", i)
		}
	}

	// the client harvests keys but cannot initiate: a client-mode start is
	// refused before any packet leaves
	if _, err := clientSvc.Jobs().StartJob(context.Background(), testCID(t)); !errors.Is(err, ErrClientMode) {
		t.Fatalf("client-mode StartJob: err = %v, want ErrClientMode", err)
	}
}

// TestServiceClientModeUnmountsHandlers: a ModeClient node advertises
// neither sphinx protocol, so at the protocol level it is indistinguishable
// from a vanilla node; it still harvests keys as a fetcher
func TestServiceClientModeUnmountsHandlers(t *testing.T) {
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Mode:  ModeClient,
		Proxy: ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()
	for _, proto := range []protocol.ID{ProtocolRelay, ProtocolKeyExchange} {
		if slices.Contains(h.Mux().Protocols(), proto) {
			t.Errorf("client-mode node advertises %s", proto)
		}
	}
}

// TestServiceRelayModeMountsHandlers: a ModeRelay node advertises both the
// relay transport and the key protocol
func TestServiceRelayModeMountsHandlers(t *testing.T) {
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Mode:  ModeRelay,
		Proxy: ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()
	for _, proto := range []protocol.ID{ProtocolRelay, ProtocolKeyExchange} {
		if !slices.Contains(h.Mux().Protocols(), proto) {
			t.Errorf("relay-mode node does not advertise %s", proto)
		}
	}
}

// TestServiceAutoModeRelayHandlerFollows: under ModeAuto the relay handler
// tracks the serving state, mounting on promotion and unmounting on
// demotion in lockstep with the key handler
func TestServiceAutoModeRelayHandlerFollows(t *testing.T) {
	fork := protocol.ID("/myfork/kad/1.0.0")
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Mode:               ModeAuto,
		KadServerProtocols: []protocol.ID{fork},
		Proxy:              ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	if slices.Contains(h.Mux().Protocols(), ProtocolRelay) {
		t.Fatal("auto-mode node advertises the relay handler before promotion")
	}
	h.SetStreamHandler(fork, func(s network.Stream) { _ = s.Reset() })
	waitFor(t, 10*time.Second, "the relay handler to mount on promotion", func() bool {
		return slices.Contains(h.Mux().Protocols(), ProtocolRelay)
	})
	h.RemoveStreamHandler(fork)
	waitFor(t, 10*time.Second, "the relay handler to unmount on demotion", func() bool {
		return !slices.Contains(h.Mux().Protocols(), ProtocolRelay)
	})
}

// TestServiceCloseSettlesPendingJobs: Close settles an in-flight job with
// ErrManagerClosed while proxies are still busy, rejects later jobs, and
// deregisters both protocol handlers
func TestServiceCloseSettlesPendingJobs(t *testing.T) {
	// Blocking discoverers keep every proxy busy past the test's lifetime,
	// so the job can only settle through Close
	cfg := ServiceConfig{
		Job:   JobConfig{Branches: 1, ReturnPaths: 1, Timeout: 30 * time.Second},
		Proxy: ProxyConfig{Timeout: 30 * time.Second},
	}
	svcs, hosts := newServiceCluster(t, jobSampleSize(1, 1)+1, &fakeDiscoverer{block: true}, cfg)
	initiator := svcs[0]
	waitForPool(t, initiator)

	res, err := initiator.Jobs().StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := initiator.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case got, ok := <-res:
		if !ok {
			t.Fatal("result channel closed without a result")
		}
		if !errors.Is(got.Err, ErrManagerClosed) {
			t.Errorf("pending job settled with %v, want ErrManagerClosed", got.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending job never settled after Close")
	}

	if _, err := initiator.Jobs().StartJob(context.Background(), testCID(t)); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("StartJob after Close: err = %v, want ErrManagerClosed", err)
	}
	for _, proto := range []protocol.ID{ProtocolRelay, ProtocolKeyExchange} {
		if slices.Contains(hosts[0].Mux().Protocols(), proto) {
			t.Errorf("handler for %s still registered after Close", proto)
		}
	}
}

// TestServiceConstructionErrors covers the constructor's validation: a
// foreign identity key and a missing discovery backend both fail fast
func TestServiceConstructionErrors(t *testing.T) {
	h := newTestHost(t)
	foreign, _ := newIdentity(t)
	if _, err := NewService(h, foreign, nil, ServiceConfig{Proxy: ProxyConfig{Discoverer: &fakeDiscoverer{}}}); err == nil {
		t.Error("NewService accepted an identity key that is not the host's")
	}
	if _, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{}); err == nil {
		t.Error("NewService accepted nil routing with no explicit discoverer")
	}
	if _, err := NewService(nil, foreign, nil, ServiceConfig{}); err == nil {
		t.Error("NewService accepted a nil host")
	}
}

// TestNewServiceInjectsHostPeerState: with no PeerState in the config,
// NewService must back the job manager from the host so the initiator-edge
// bias runs by default
func TestNewServiceInjectsHostPeerState(t *testing.T) {
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Proxy: ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()
	if svc.jobs.state == nil {
		t.Fatal("NewService left the job manager PeerState nil")
	}
}

// TestServiceKadServerProtocolsThreaded: a custom KadServerProtocols list
// passed through ServiceConfig must reach the underlying KeyExchange, not
// just fall back to DefaultKadServerProtocols
func TestServiceKadServerProtocolsThreaded(t *testing.T) {
	fork := protocol.ID("/myfork/kad/1.0.0")
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Mode:               ModeAuto,
		KadServerProtocols: []protocol.ID{fork},
		Proxy:              ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	if got := svc.Mode(); got != ModeClient {
		t.Fatalf("initial Mode() = %v, want ModeClient", got)
	}

	h.SetStreamHandler(fork, func(s network.Stream) { _ = s.Reset() })
	waitFor(t, 10*time.Second, "promotion via the configured kad protocol", func() bool {
		return svc.Mode() == ModeRelay
	})
}

// TestServicePoolStatus: live tracks the pool through fills and expiry,
// need is one attempt's demand at the configured k and m
func TestServicePoolStatus(t *testing.T) {
	h := newTestHost(t)
	svc, err := NewService(h, h.Peerstore().PrivKey(h.ID()), nil, ServiceConfig{
		Job:   JobConfig{Branches: 1, ReturnPaths: 1},
		Proxy: ProxyConfig{Discoverer: &fakeDiscoverer{}},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Close()

	if live, need := svc.PoolStatus(); live != 0 || need != jobSampleSize(1, 1) {
		t.Errorf("empty pool status = %d live / %d need, want 0 / %d", live, need, jobSampleSize(1, 1))
	}
	fillPool(t, svc.pool, 2)
	if live, _ := svc.PoolStatus(); live != 2 {
		t.Errorf("filled pool reports %d live, want 2", live)
	}
	expirePool(svc.pool)
	if live, _ := svc.PoolStatus(); live != 0 {
		t.Errorf("expired pool reports %d live, want 0", live)
	}
}
