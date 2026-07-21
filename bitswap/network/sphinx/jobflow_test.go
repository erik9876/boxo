package sphinx

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// jobNode is one fully wired full node: packet core plus both job-layer
// roles (proxy and initiator) behind a DeliveryMux
type jobNode struct {
	host  host.Host
	km    *KeyManager
	surbs *SURBStore
	pool  *KeyStore
	jobs  *JobManager
}

func newJobNode(t *testing.T, disc ProviderDiscoverer, jobTimeout time.Duration) *jobNode {
	t.Helper()
	h := newTestHost(t)
	idPriv := h.Peerstore().PrivKey(h.ID())
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	surbs := NewSURBStore()
	pool := NewKeyStore()

	// The mux exists before relay and transport; the roles are installed
	// afterwards because they send through the transport
	mux := &DeliveryMux{}
	relay, err := NewRelay(km, surbs, mux)
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	tr := NewTransport(h, relay, nil)
	t.Cleanup(func() { tr.Close() })

	proxy, err := NewProxy(tr, ProxyConfig{Discoverer: disc, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewProxy: %v", err)
	}
	jobs, err := NewJobManager(tr, km, pool, surbs, JobConfig{Timeout: jobTimeout})
	if err != nil {
		t.Fatalf("NewJobManager: %v", err)
	}
	mux.SetProxy(proxy)
	mux.SetJobs(jobs)

	return &jobNode{host: h, km: km, surbs: surbs, pool: pool, jobs: jobs}
}

// newJobCluster wires an initiator plus enough pool nodes for one attempt
// at the default k and m, connects every pair, and teaches the initiator's
// pool all other nodes, as the key exchange would have
func newJobCluster(t *testing.T, disc ProviderDiscoverer, jobTimeout time.Duration) (initiator *jobNode, poolNodes []*jobNode) {
	t.Helper()
	initiator = newJobNode(t, disc, jobTimeout)
	poolNodes = make([]*jobNode, jobSampleSize(DefaultBranchesPerJob, ReturnPathsPerJob))
	hosts := []host.Host{initiator.host}
	for i := range poolNodes {
		poolNodes[i] = newJobNode(t, disc, jobTimeout)
		hosts = append(hosts, poolNodes[i].host)
	}
	connectAll(t, hosts...)

	// Only the initiator samples; its pool learns every other node
	for _, n := range poolNodes {
		if err := initiator.pool.Put(NewKeyRecord(n.km.PeerID(), n.km.PublicKey(), time.Hour)); err != nil {
			t.Fatalf("filling initiator pool: %v", err)
		}
	}
	return initiator, poolNodes
}

// runJobToSuccess starts one job and asserts it settles with want, leaving
// no SURB entries or job state behind
func runJobToSuccess(t *testing.T, initiator *jobNode, want []peer.AddrInfo) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := initiator.jobs.StartJob(ctx, testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	select {
	case got, ok := <-res:
		if !ok {
			t.Fatal("result channel closed without a result")
		}
		if got.Err != nil {
			t.Fatalf("job settled with error: %v", got.Err)
		}
		assertProvidersEqual(t, got.Providers, want)
	case <-time.After(30 * time.Second):
		t.Fatal("job never settled")
	}

	// Success cleaned everything: no SURB entries (used or unused), no job
	// state
	if initiator.surbs.Len() != 0 {
		t.Errorf("initiator surb store holds %d entries after success", initiator.surbs.Len())
	}
	if jobs, surbIDs := initiator.jobs.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("initiator job state after success: %d jobs / %d surb ids", jobs, surbIDs)
	}
}

// full loopback flow at k = 2: both branches through three hops to their
// proxies, fake discovery, the two identical answers complete the quorum
// and merge byte-identically, later copies die as duplicates
func TestDiscoveryJobEndToEnd(t *testing.T) {
	want := testProviders(t, 3)
	disc := &fakeDiscoverer{providers: want}

	// k·(NrHops + 2m) = 18 distinct pool relays at the defaults; with the
	// initiator that is nineteen hosts
	initiator, _ := newJobCluster(t, disc, 20*time.Second)
	runJobToSuccess(t, initiator, want)
}

// TestJobSurvivesDeadRelay kills one pool node outright before the job
// starts. Full per-job disjointness confines the dead relay to exactly one
// branch, whether as first hop (the send fails, a tolerated dead branch
// that leaves the quorum to the live one) or a silent packet loss further
// along (the live branch's answer settles at the attempt timer, hence the
// short timeout here), so the job must settle without any retransmit
func TestJobSurvivesDeadRelay(t *testing.T) {
	want := testProviders(t, 3)
	disc := &fakeDiscoverer{providers: want}

	// Pool exactly one attempt's demand, so the dead node is drawn with
	// certainty
	initiator, poolNodes := newJobCluster(t, disc, 2*time.Second)
	if err := poolNodes[0].host.Close(); err != nil {
		t.Fatalf("killing pool node: %v", err)
	}

	runJobToSuccess(t, initiator, want)
}
