package sphinx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// DefaultProxyTimeout bounds one job's discovery at the proxy. Must stay
// well below the initiator's job timeout. Placeholder until calibration
const DefaultProxyTimeout = 10 * time.Second

// DefaultMaxConcurrentJobs caps parallel discovery jobs. Over the cap jobs
// are dropped, not queued (a queue would be unbounded state); the
// initiator's timeout absorbs the loss
const DefaultMaxConcurrentJobs = 64

// ProviderDiscoverer is the discovery backend a proxy runs jobs against
type ProviderDiscoverer interface {
	// fewer than limit, or none, is a valid result; an error means
	// discovery itself failed
	FindProviders(ctx context.Context, c cid.Cid, limit int) ([]peer.AddrInfo, error)
}

// PacketSender is the transport seam; Transport satisfies it
type PacketSender interface {
	SendPacket(ctx context.Context, next peer.ID, pkt []byte) error
}

// ContentRoutingDiscoverer adapts routing.ContentRouting (the DHT) to
// ProviderDiscoverer. A timeout returns what was found, not an error
type ContentRoutingDiscoverer struct {
	Routing routing.ContentRouting
}

func (d ContentRoutingDiscoverer) FindProviders(ctx context.Context, c cid.Cid, limit int) ([]peer.AddrInfo, error) {
	if d.Routing == nil {
		return nil, errors.New("content routing discoverer has no backend")
	}
	found := make([]peer.AddrInfo, 0, limit)
	for p := range d.Routing.FindProvidersAsync(ctx, c, limit) {
		if p.ID == "" {
			continue
		}
		found = append(found, p)
	}
	return found, nil
}

type ProxyConfig struct {
	// required
	Discoverer ProviderDiscoverer
	// zero means DefaultProxyTimeout
	Timeout time.Duration
	// zero means DefaultMaxConcurrentJobs
	MaxConcurrentJobs int
}

// Proxy executes delivered discovery jobs: decode, discover, answer
// through every SURB. No state across jobs beyond the running counter;
// never fetches blocks
type Proxy struct {
	sender  PacketSender
	disc    ProviderDiscoverer
	timeout time.Duration
	maxJobs int

	metrics ProxyMetrics

	mu      sync.Mutex
	running int
}

func (p *Proxy) Metrics() ProxyMetricsSnapshot {
	return p.metrics.Snapshot()
}

func NewProxy(sender PacketSender, cfg ProxyConfig) (*Proxy, error) {
	if sender == nil {
		return nil, errors.New("proxy needs a packet sender")
	}
	if cfg.Discoverer == nil {
		return nil, errors.New("proxy needs a provider discoverer")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultProxyTimeout
	}
	if cfg.MaxConcurrentJobs == 0 {
		cfg.MaxConcurrentJobs = DefaultMaxConcurrentJobs
	}
	if cfg.Timeout < 0 || cfg.MaxConcurrentJobs < 0 {
		return nil, errors.New("proxy timeout and job cap must be positive")
	}
	return &Proxy{
		sender:  sender,
		disc:    cfg.Discoverer,
		timeout: cfg.Timeout,
		maxJobs: cfg.MaxConcurrentJobs,
	}, nil
}

// HandleDelivery runs synchronously inside ProcessPacket, so it only
// decodes and admits; discovery and replying run on their own goroutine
func (p *Proxy) HandleDelivery(recipient RecipientID, payload []byte) {
	if recipient != DiscoveryRecipient {
		log.Debugw("dropping delivery for unserved recipient", "recipient", fmt.Sprintf("%x", recipient))
		return
	}
	job, err := DecodeJob(payload)
	if err != nil {
		log.Debugw("dropping undecodable discovery job", "error", err)
		return
	}
	if !p.admit() {
		log.Debugw("dropping discovery job over concurrency cap", "cid", job.CID)
		return
	}
	go p.runJob(job)
}

func (p *Proxy) runJob(job *Job) {
	defer p.release()
	p.metrics.JobsExecuted.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	// a ProbeThenRouteDiscoverer runs the vanilla two-stage lookup here:
	// WANT-HAVE to the neighbors first, the DHT only on an empty probe
	status := ReplyStatusOK
	providers, err := p.disc.FindProviders(ctx, job.CID, MaxProvidersPerReply)
	if err != nil {
		log.Debugw("discovery failed at proxy", "cid", job.CID, "error", err)
		status, providers = ReplyStatusFailed, nil
	}

	reply, err := EncodeReply(status, capProviders(providers))
	if err != nil {
		// unreachable after capProviders
		log.Warnw("encoding discovery reply failed", "cid", job.CID, "error", err)
		return
	}

	// identical reply through every SURB; the initiator dedups by SURB ID.
	// A failed send is a benign drop (that is what the m return paths pay
	// for). Own deadline so the discovery timeout cannot cut off the send
	for i, surb := range job.SURBs {
		pkt, first, err := NewReplyFromSURB(surb, reply)
		if err != nil {
			log.Debugw("skipping unusable surb", "cid", job.CID, "surb", i, "error", err)
			continue
		}
		sctx, scancel := context.WithTimeout(context.Background(), relayTimeout)
		if err := p.sender.SendPacket(sctx, first, pkt); err != nil {
			log.Debugw("discovery reply dropped", "cid", job.CID, "surb", i, "error", err)
		}
		scancel()
	}
}

func (p *Proxy) admit() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running >= p.maxJobs {
		return false
	}
	p.running++
	return true
}

func (p *Proxy) release() {
	p.mu.Lock()
	p.running--
	p.mu.Unlock()
}
