package main

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// observedRouting reports each FindProvidersAsync walk to the observer:
// when the first provider record surfaced and when the walk ended. The
// arrival instants in job_results sit behind the full drain and the PQM's
// connect check, so this is the only place the "record found" moment shows
type observedRouting struct {
	inner    routing.ContentRouting
	observer func(c cid.Cid, start, firstRecord, end time.Time, providers int)
}

func (o observedRouting) Provide(ctx context.Context, c cid.Cid, announce bool) error {
	return o.inner.Provide(ctx, c, announce)
}

func (o observedRouting) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	start := time.Now()
	src := o.inner.FindProvidersAsync(ctx, c, count)
	out := make(chan peer.AddrInfo)
	go func() {
		var firstRecord time.Time
		providers := 0
		// blocking sends: the discoverer drains to close unconditionally,
		// and a ctx escape here would race it into dropping records
		for p := range src {
			if p.ID != "" {
				if firstRecord.IsZero() {
					firstRecord = time.Now()
				}
				providers++
			}
			out <- p
		}
		end := time.Now()
		close(out)
		if o.observer != nil {
			o.observer(c, start, firstRecord, end, providers)
		}
	}()
	return out
}
