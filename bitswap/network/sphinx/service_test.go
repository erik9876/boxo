package sphinx

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	rpqm "github.com/ipfs/boxo/routing/providerquerymanager"
	"github.com/libp2p/go-libp2p/core/host"
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
