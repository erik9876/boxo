package main

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/testground/sdk-go/runtime"
)

// jobResult is one discovery job as the harness saw it; the stopwatch
// starts at the FindProvidersAsync call. Outcomes are success (a provider
// arrived), timeout (the job ctx expired first) and failed (the channel
// closed empty with the ctx still live). Millisecond fields hold -1 when
// the instant never happened; all of them sit on the initiator's clock
type jobResult struct {
	Idx              int             `json:"idx"`
	Outcome          string          `json:"outcome"`
	DurationMs       float64         `json:"duration_ms"`
	FirstMs          float64         `json:"first_ms"`
	SettledMs        float64         `json:"settled_ms"`
	ProbeFirstHaveMs float64         `json:"probe_first_have_ms"`
	DhtFirstRecordMs float64         `json:"dht_first_record_ms"`
	SettleKind       string          `json:"settle_kind,omitempty"`
	Providers        int             `json:"providers"`
	CtxExpired       bool            `json:"ctx_expired"`
	LocalRecord      bool            `json:"local_record"`
	Attempts         []attemptExport `json:"attempts,omitempty"`
}

// eventDrain gives laggard observer emits time to land before assembly
const eventDrain = 200 * time.Millisecond

// runJobs is the sequential job loop on the initiator. Queries run to
// their natural end, without cancelling on the first provider, so the
// settled instant is measured
func runJobs(ctx context.Context, nd *node, runenv *runtime.RunEnv, evlog *eventLog, cids []cid.Cid, budget time.Duration) []jobResult {
	results := make([]jobResult, 0, len(cids))
	starts := make([]time.Time, len(cids))
	for i, c := range cids {
		res := jobResult{Idx: i, FirstMs: -1, SettledMs: -1, ProbeFirstHaveMs: -1,
			DhtFirstRecordMs: -1, DurationMs: -1, LocalRecord: hasLocalRecord(ctx, nd, c)}

		jctx, cancel := context.WithTimeout(ctx, budget)
		start := time.Now()
		starts[i] = start
		ch := nd.pqm.FindProvidersAsync(jctx, c, 0)
		var firstAt time.Time
		for range ch {
			if res.Providers == 0 {
				firstAt = time.Now()
			}
			res.Providers++
		}
		closedAt := time.Now()
		expired := jctx.Err() != nil
		cancel()

		res.DurationMs = msBetween(start, firstAt)
		res.FirstMs = res.DurationMs
		res.SettledMs = msBetween(start, closedAt)
		switch {
		case res.Providers > 0:
			res.Outcome = "success"
			res.CtxExpired = expired
		case expired:
			res.Outcome = "timeout"
			res.CtxExpired = true
		default:
			res.Outcome = "failed"
		}
		results = append(results, res)
		runenv.RecordMessage("job %d: %s after %.1f ms (%d providers, settled %.1f ms, local_record=%t)",
			i, res.Outcome, res.DurationMs, res.Providers, res.SettledMs, res.LocalRecord)
	}

	time.Sleep(eventDrain)
	for i := range results {
		r := &results[i]
		c, start := cids[r.Idx], starts[r.Idx]
		if nd.svc != nil {
			attempts, firstOK, settled, kind := evlog.exportJob(c, start)
			r.Attempts, r.SettleKind = attempts, kind
			if firstOK >= 0 {
				r.FirstMs = firstOK
			}
			if settled >= 0 {
				r.SettledMs = settled
			}
		} else {
			r.ProbeFirstHaveMs = evlog.probeFirstHaveMs(c, start)
			if r.ProbeFirstHaveMs >= 0 && (r.FirstMs < 0 || r.ProbeFirstHaveMs < r.FirstMs) {
				r.FirstMs = r.ProbeFirstHaveMs
			}
			// the walk's own "record found" instant; first_ms stays the
			// later channel arrival
			r.DhtFirstRecordMs = evlog.dhtFirstRecordMs(c, start)
		}
	}
	return results
}

// checked before the lookup runs, so the check cannot warm anything
func hasLocalRecord(ctx context.Context, nd *node, c cid.Cid) bool {
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	provs, err := nd.dht.ProviderStore().GetProviders(sctx, c.Hash())
	return err == nil && len(provs) > 0
}
