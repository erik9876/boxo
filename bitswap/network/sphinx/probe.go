package sphinx

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// the prober rides on the bitswap message fan-out
// (bitswap.WithExtraReceivers)
var _ bsnet.Receiver = (*WantHaveProber)(nil)

// DefaultProbeWindow is how long a probe collects HAVE replies, matching
// vanilla's ProvSearchDelay (the broadcast-to-DHT gap of a bitswap
// session). Placeholder until calibration
const DefaultProbeWindow = time.Second

// a fresh session's first want carries the highest priority; the probe
// copies that so its broadcast looks like any other first want
const probeWantPriority = math.MaxInt32

// bounds one fire-and-forget message send
const probeSendTimeout = 10 * time.Second

// WantSender sends one bitswap message; bsnet.BitSwapNetwork satisfies it
type WantSender interface {
	SendMessage(ctx context.Context, p peer.ID, msg bsmsg.BitSwapMessage) error
}

// NeighborSource lists the peers a probe asks and their known addresses;
// HostNeighbors adapts a live host
type NeighborSource interface {
	Peers() []peer.ID
	Addrs(p peer.ID) []ma.Multiaddr
}

// HostNeighbors feeds a prober from a live host: the connected peers and
// their peerstore addresses
type HostNeighbors struct{ Host host.Host }

func (h HostNeighbors) Peers() []peer.ID               { return h.Host.Network().Peers() }
func (h HostNeighbors) Addrs(p peer.ID) []ma.Multiaddr { return h.Host.Peerstore().Addrs(p) }

type ProbeConfig struct {
	// zero means DefaultProbeWindow
	Window time.Duration
}

// WantHaveProber runs the neighbor half of a proxy's provider discovery:
// one WANT-HAVE broadcast to the connected peers, exactly the opening move
// of a vanilla bitswap session (no SendDontHave, no WANT-BLOCK, cancel
// afterwards). Plugged into the network as an extra receiver
// (bitswap.WithExtraReceivers) it observes HAVEs without touching client
// or server state
type WantHaveProber struct {
	sender    WantSender
	neighbors NeighborSource
	window    time.Duration

	mu sync.Mutex
	// pending probes per CID; concurrent probes for one CID each get
	// their own watch and all feed from the same replies
	watches map[cid.Cid][]*probeWatch
}

// probeWatch collects the responders of one Probe call
type probeWatch struct {
	limit int
	seen  map[peer.ID]struct{}
	// responders in arrival order; the limit caps it
	order []peer.ID
	// closed when the limit is reached
	full chan struct{}
}

func NewWantHaveProber(sender WantSender, neighbors NeighborSource, cfg ProbeConfig) *WantHaveProber {
	if cfg.Window == 0 {
		cfg.Window = DefaultProbeWindow
	}
	return &WantHaveProber{
		sender:    sender,
		neighbors: neighbors,
		window:    cfg.Window,
		watches:   make(map[cid.Cid][]*probeWatch),
	}
}

// Probe broadcasts a WANT-HAVE for c to the current neighbors and collects
// the peers that answer HAVE (or send the block outright, see
// ReceiveMessage), until limit responders or the window end. Addresses
// come from the NeighborSource; responders are connected, so the entries
// are dialable without a lookup. An empty result is normal: the caller
// decides about a fallback
func (p *WantHaveProber) Probe(ctx context.Context, c cid.Cid, limit int) []peer.AddrInfo {
	if limit <= 0 {
		return nil
	}
	neighbors := p.neighbors.Peers()
	if len(neighbors) == 0 {
		return nil
	}

	w := &probeWatch{
		limit: limit,
		seen:  make(map[peer.ID]struct{}),
		full:  make(chan struct{}),
	}
	p.mu.Lock()
	p.watches[c] = append(p.watches[c], w)
	p.mu.Unlock()
	defer p.unregister(c, w)

	// one shared read-only message; sends are fire-and-forget and stop
	// mattering at window end
	want := bsmsg.New(false)
	want.AddEntry(c, probeWantPriority, pb.Message_Wantlist_Have, false)
	sendCtx, stopSends := context.WithCancel(context.Background())
	defer stopSends()
	p.broadcast(sendCtx, neighbors, want)

	timer := time.NewTimer(p.window)
	defer timer.Stop()
	select {
	case <-w.full:
	case <-timer.C:
	case <-ctx.Done():
	}

	// wantlist hygiene: a vanilla client cancels a satisfied broadcast
	// want at every peer that got it, so the probe does too. Detached
	// from ctx; a cancel for a want that never arrived is a remote no-op
	cancelMsg := bsmsg.New(false)
	cancelMsg.Cancel(c)
	p.broadcast(context.Background(), neighbors, cancelMsg)

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(w.order) == 0 {
		return nil
	}
	providers := make([]peer.AddrInfo, 0, len(w.order))
	for _, pid := range w.order {
		providers = append(providers, peer.AddrInfo{ID: pid, Addrs: p.neighbors.Addrs(pid)})
	}
	return providers
}

// broadcast sends msg to every neighbor, one goroutine per peer like the
// per-peer message queues of a vanilla client; failed sends are benign
// drops
func (p *WantHaveProber) broadcast(ctx context.Context, neighbors []peer.ID, msg bsmsg.BitSwapMessage) {
	for _, n := range neighbors {
		go func(n peer.ID) {
			sctx, cancel := context.WithTimeout(ctx, probeSendTimeout)
			defer cancel()
			if err := p.sender.SendMessage(sctx, n, msg); err != nil {
				log.Debugw("probe message dropped", "peer", n, "error", err)
			}
		}(n)
	}
}

func (p *WantHaveProber) unregister(c cid.Cid, w *probeWatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ws := p.watches[c]
	for i, cand := range ws {
		if cand == w {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(p.watches, c)
	} else {
		p.watches[c] = ws
	}
}

// ReceiveMessage feeds inbound bitswap traffic into pending probes. HAVEs
// count their sender as provider; so does a full block, because the server
// answers a WANT-HAVE for a small block with the block itself
// (engine.maxBlockSizeReplaceHasWithBlock) and the client keeps it out of
// the proxy's stores by never having asked for it
func (p *WantHaveProber) ReceiveMessage(_ context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	cids := incoming.Haves()
	for _, b := range incoming.Blocks() {
		cids = append(cids, b.Cid())
	}
	if len(cids) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cids {
		for _, w := range p.watches[c] {
			w.record(sender)
		}
	}
}

// record adds sender once; the limit closes full. Caller holds the
// prober's mu
func (w *probeWatch) record(sender peer.ID) {
	if len(w.order) >= w.limit {
		return
	}
	if _, dup := w.seen[sender]; dup {
		return
	}
	w.seen[sender] = struct{}{}
	w.order = append(w.order, sender)
	if len(w.order) == w.limit {
		close(w.full)
	}
}

func (p *WantHaveProber) ReceiveError(error)       {}
func (p *WantHaveProber) PeerConnected(peer.ID)    {}
func (p *WantHaveProber) PeerDisconnected(peer.ID) {}

// Prober is the probe seam of ProbeThenRouteDiscoverer; *WantHaveProber
// satisfies it
type Prober interface {
	Probe(ctx context.Context, c cid.Cid, limit int) []peer.AddrInfo
}

// ProbeThenRouteDiscoverer implements a proxy's two-stage vanilla lookup:
// WANT-HAVE to the neighbors first, the routing backend (DHT) only when
// the probe comes back empty
type ProbeThenRouteDiscoverer struct {
	Probe Prober
	Route ProviderDiscoverer
}

func (d ProbeThenRouteDiscoverer) FindProviders(ctx context.Context, c cid.Cid, limit int) ([]peer.AddrInfo, error) {
	if d.Probe != nil {
		if found := d.Probe.Probe(ctx, c, limit); len(found) > 0 {
			return found, nil
		}
	}
	if d.Route == nil {
		return nil, errors.New("probe discoverer has no routing backend")
	}
	return d.Route.FindProviders(ctx, c, limit)
}
