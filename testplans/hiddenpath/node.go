package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	client "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
	blockstore "github.com/ipfs/boxo/blockstore"
	rpqm "github.com/ipfs/boxo/routing/providerquerymanager"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// uniformFinder forces a full Kademlia walk on every pre-send lookup. The
// stock FindPeer answers connected peers from FindLocal without touching
// the network, exactly the variation the pre-send lookup removes;
// GetClosestPeers has no such shortcut and fills the peerstore on the way
type uniformFinder struct {
	dht  *dht.IpfsDHT
	host host.Host
}

func (f uniformFinder) FindPeer(ctx context.Context, p peer.ID) (peer.AddrInfo, error) {
	if _, err := f.dht.GetClosestPeers(ctx, string(p)); err != nil {
		return peer.AddrInfo{}, err
	}
	return f.host.Peerstore().PeerInfo(p), nil
}

// node is one instance's stack: host, DHT, sphinx service, explicit PQM,
// bitswap with the internal PQM off. Vanilla mode builds no sphinx
// service, whose key-exchange traffic would contaminate the byte comparison
type node struct {
	host   host.Host
	bwc    *metrics.BandwidthCounter
	dht    *dht.IpfsDHT
	svc    *sphinx.Service // nil in vanilla mode
	pqm    *rpqm.ProviderQueryManager
	bstore blockstore.Blockstore
	bswap  *bitswap.Bitswap
	jobCfg sphinx.JobConfig

	// dht client without key serving; relays and proxies never draw it
	isClient bool

	// anchors the refresh-free window: the first key refresh fires at
	// TTL/8 = 7.5 min, so the job loop must end before that
	windowStart time.Time
}

// per-job deadline: one attempt in vanilla mode, worst-case job lifetime
// in sphinx mode. The exported per-attempt records give the budget-equal
// comparison
func (nd *node) jobBudget(p params) time.Duration {
	if nd.svc != nil {
		return sphinx.SuggestedFinderTimeout(nd.jobCfg)
	}
	return p.Timeout
}

// walkEmitMargin is how much of the finder deadline is reserved for
// handing found records over instead of walking
const walkEmitMargin = time.Second

// discovererFinder adapts the two-stage vanilla discovery to the
// ContentDiscovery interface the PQM consumes
type discovererFinder struct {
	disc sphinx.ProviderDiscoverer
}

func (d discovererFinder) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		if count <= 0 {
			count = sphinx.MaxProvidersPerReply
		}
		// under churn the walk drags on dead-peer dials; cut it with an
		// emission margin left, a cut walk returns what it found
		walkCtx := ctx
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			walkCtx, cancel = context.WithDeadline(ctx, deadline.Add(-walkEmitMargin))
			defer cancel()
		}
		provs, err := d.disc.FindProviders(walkCtx, c, count)
		if err != nil {
			return
		}
		for _, p := range provs {
			select {
			case out <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// buildNode constructs the stack. The sphinx service must exist before
// mesh connect: pools fill via onConnected, earlier connections wait for
// the refresh loop
func buildNode(ctx context.Context, p params, isClient bool, evlog *eventLog) (*node, error) {
	nd := &node{isClient: isClient}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating identity: %w", err)
	}
	nd.bwc = metrics.NewBandwidthCounter()
	nd.host, err = libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.BandwidthReporter(nd.bwc),
	)
	if err != nil {
		return nil, fmt.Errorf("constructing host: %w", err)
	}

	dhtMode := dht.ModeServer
	if isClient {
		dhtMode = dht.ModeClient
	}
	nd.dht, err = dht.New(ctx, nd.host, dht.Mode(dhtMode))
	if err != nil {
		nd.close()
		return nil, fmt.Errorf("constructing dht: %w", err)
	}

	nd.jobCfg = sphinx.JobConfig{
		Timeout:              p.Timeout,
		ReturnPaths:          p.M,
		Branches:             p.K,
		MinProxyCPL:          p.L,
		DisableRetransmit:    !p.Retransmit,
		Draw:                 p.Draw,
		InitiatorEdgeEpsilon: p.EdgeEpsilon,
	}

	var finder routing.ContentDiscovery
	finderTimeout := p.Timeout
	nd.windowStart = time.Now()
	net := bsnet.NewFromIpfsHost(nd.host)
	bsOpts := []bitswap.Option{
		bitswap.WithClientOption(client.WithDefaultProviderQueryManager(false)),
	}

	// both modes run the identical two-stage discovery, the proxy remotely
	// and the vanilla initiator locally
	prober := sphinx.NewWantHaveProber(net, sphinx.HostNeighbors{Host: nd.host},
		sphinx.ProbeConfig{Observer: evlog.onProbe})
	bsOpts = append(bsOpts, bitswap.WithExtraReceivers(prober))
	twoStage := sphinx.ProbeThenRouteDiscoverer{
		Probe: prober,
		Route: sphinx.ContentRoutingDiscoverer{
			Routing: observedRouting{inner: nd.dht, observer: evlog.onWalk},
		},
	}

	if p.sphinx() {
		sphinxMode := sphinx.ModeRelay
		if isClient {
			sphinxMode = sphinx.ModeClient
		}
		nd.jobCfg.Observer = evlog
		nd.svc, err = sphinx.NewService(nd.host, priv, nd.dht, sphinx.ServiceConfig{
			Job: nd.jobCfg,
			Proxy: sphinx.ProxyConfig{
				Timeout:    p.ProxyTimeout,
				Discoverer: twoStage,
			},
			PeerRouting: uniformFinder{dht: nd.dht, host: nd.host},
			Mode:        sphinxMode,
		})
		if err != nil {
			nd.close()
			return nil, fmt.Errorf("constructing sphinx service: %w", err)
		}
		finder = sphinx.NewProviderFinder(nd.svc.Jobs())
		finderTimeout = sphinx.SuggestedFinderTimeout(nd.jobCfg)
	} else {
		finder = discovererFinder{disc: twoStage}
	}

	nd.pqm, err = rpqm.New(net, finder,
		rpqm.WithMaxTimeout(finderTimeout),
		rpqm.WithMaxProviders(sphinx.MaxProvidersPerReply))
	if err != nil {
		nd.close()
		return nil, fmt.Errorf("constructing provider query manager: %w", err)
	}

	nd.bstore = blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	nd.bswap = bitswap.New(ctx, net, nd.pqm, nd.bstore, bsOpts...)

	return nd, nil
}

// reverse construction order; victims never call it
func (nd *node) close() {
	if nd.bswap != nil {
		_ = nd.bswap.Close()
	}
	if nd.pqm != nil {
		nd.pqm.Close()
	}
	if nd.svc != nil {
		_ = nd.svc.Close()
	}
	if nd.dht != nil {
		_ = nd.dht.Close()
	}
	if nd.host != nil {
		_ = nd.host.Close()
	}
}
