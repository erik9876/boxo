package sphinx

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// JobStarter is the seam the ProviderFinder consumes; *JobManager
// satisfies it
type JobStarter interface {
	StartJob(ctx context.Context, c cid.Cid) (<-chan JobResult, error)
}

// ProviderFinder adapts the job layer to routing.ContentDiscovery, the
// interface Bitswap's client consumes: one FindProvidersAsync = one job.
// Bitswap needs nothing beyond FindProvidersAsync, so there is no Provide
// stub; announcing own content stays on the vanilla provider system
type ProviderFinder struct {
	jobs JobStarter
}

func NewProviderFinder(jobs JobStarter) *ProviderFinder {
	return &ProviderFinder{jobs: jobs}
}

// FindProvidersAsync launches one job for c and streams the job's merged
// providers (at most count, all for count <= 0), then closes the
// channel. Every failure = empty closed channel; no fallback to vanilla
// discovery. A dying ctx stops the wait but not the job, which settles on
// its own timers
func (f *ProviderFinder) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	res, err := f.jobs.StartJob(ctx, c)
	if err != nil {
		log.Debugw("sphinx provider lookup not started", "cid", c, "error", err)
		close(out)
		return out
	}
	go func() {
		defer close(out)
		select {
		case r, ok := <-res:
			if !ok {
				return
			}
			if r.Err != nil {
				log.Debugw("sphinx provider lookup failed", "cid", c, "error", r.Err)
				return
			}
			for i, p := range r.Providers {
				if count > 0 && i == count {
					return
				}
				select {
				case out <- p:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
		}
	}()
	return out
}

// SuggestedFinderTimeout is the per-query timeout the layer above must
// allow: two attempts plus grace for the final return path. Bitswap's
// default ProviderQueryManager caps at 10 s, far under a job's worst
// case, so construct the manager explicitly:
//
//	pqm, _ := providerquerymanager.New(net, finder,
//	    providerquerymanager.WithMaxTimeout(sphinx.SuggestedFinderTimeout(cfg.Job)),
//	    providerquerymanager.WithMaxProviders(sphinx.MaxProvidersPerReply))
//	bs := bitswap.New(ctx, net, pqm, bstore,
//	    client.WithDefaultProviderQueryManager(false),
//	    client.BroadcastControlEnable(true),
//	    client.BroadcastControlMaxPeers(0))
//
// The BroadcastControl pair matters once blocks are fetched through
// sessions: without it every want also leaves as a plaintext WANT-HAVE
// broadcast to all connected peers, ahead of the sphinx lookup. For
// per-session instead of client-wide suppression, fetch through
// client.NewSessionWithOptions with client.WithAnonymousDiscovery(pqm).
func SuggestedFinderTimeout(cfg JobConfig) time.Duration {
	t := cfg.Timeout
	if t == 0 {
		t = DefaultJobTimeout
	}
	return 2*t + 5*time.Second
}
