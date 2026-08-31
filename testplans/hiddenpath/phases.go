package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/multiformats/go-multihash"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"golang.org/x/sync/errgroup"
)

// barrier states in run order; targets are N until pre-kill, N-f after
const (
	stateMeshReady = "mesh-ready"
	stateDHTReady  = "dht-ready"
	stateProvided  = "provided"
	statePoolsFull = "pools-full"
	statePreKill   = "pre-kill"
	statePostKill  = "post-kill"
	stateJobsDone  = "jobs-done"
	stateDone      = "done"
)

const (
	connectTimeout     = 10 * time.Second
	connectAttempts    = 8
	connectParallel    = 4
	connectPeerBudget  = 60 * time.Second
	connectBackoffBase = 750 * time.Millisecond
	connectStaggerMax  = 5 * time.Second
	publishTimeout     = 60 * time.Second
	rosterTimeout      = 90 * time.Second
	rtDeadline         = 30 * time.Second
	provideTimeout     = 30 * time.Second
	poolDeadline       = 120 * time.Second
	poolBounceAfter    = 15 * time.Second
	postKillSettle     = 3 * time.Second
	bwFlushWait        = 2 * time.Second
	measurementCap     = 7 * time.Minute
	// bounds one SignalAndWait; jobs-done gets a computed cap instead
	barrierCap      = 5 * time.Minute
	victimSeedSalt  = 0x68705f6b696c6c // "hp_kill"; decouples the victim draw from other seed uses
	connectSeedSalt = 0x68705f6d657368 // "hp_mesh"; decouples connect jitter from other seed uses
)

// one instance on the sync topic: seq carries role and connect direction
type rosterEntry struct {
	Seq  int64
	Info peer.AddrInfo
}

// barrier signals state and waits for target instances. Deadline-bounded
// so a wedged sync stream surfaces as an error naming the barrier
func barrier(ctx context.Context, client sync.Client, name string, target int, within time.Duration) error {
	bctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	if _, err := client.SignalAndWait(bctx, sync.State(name), target); err != nil {
		return fmt.Errorf("barrier %q (target %d): %w", name, target, err)
	}
	return nil
}

// publishAndCollectRoster announces this instance on the "peers" topic and
// blocks until every entry arrived; callers filter by Seq
func publishAndCollectRoster(ctx context.Context, runenv *runtime.RunEnv, client sync.Client, self rosterEntry) ([]rosterEntry, error) {
	topic := sync.NewTopic("peers", &rosterEntry{})
	pctx, pcancel := context.WithTimeout(ctx, publishTimeout)
	_, err := client.Publish(pctx, topic, &self)
	pcancel()
	if err != nil {
		return nil, fmt.Errorf("publishing roster entry: %w", err)
	}

	n := runenv.TestInstanceCount
	ch := make(chan *rosterEntry, n)
	sctx, cancel := context.WithTimeout(ctx, rosterTimeout)
	defer cancel()
	if _, err := client.Subscribe(sctx, topic, ch); err != nil {
		return nil, fmt.Errorf("subscribing to roster: %w", err)
	}

	roster := make([]rosterEntry, 0, n)
	seen := make(map[int64]bool, n)
	for len(roster) < n {
		select {
		case e := <-ch:
			if e == nil || seen[e.Seq] {
				continue
			}
			seen[e.Seq] = true
			roster = append(roster, *e)
		case <-sctx.Done():
			return nil, fmt.Errorf("roster incomplete after %s: %d of %d entries", rosterTimeout, len(roster), n)
		}
	}
	return roster, nil
}

// meshStats is the bootstrap outcome for the result JSON
type meshStats struct {
	Targets    int
	Connected  int
	FailedSeqs []int64
}

// connectMesh dials every roster peer with a higher seq, so each unordered
// pair connects once and fires onConnected on both ends. Edges fail soft:
// a node fails bootstrap only below ceil(mesh_quorum * targets). Jitter
// comes from per-peer RNGs seeded from (seed, selfSeq, peer seq), so the
// dial order is reproducible regardless of scheduling
func connectMesh(ctx context.Context, nd *node, runenv *runtime.RunEnv, roster []rosterEntry, selfSeq int64, p params) (meshStats, error) {
	staggerRng := rand.New(rand.NewSource(int64(p.Seed) ^ connectSeedSalt ^ (selfSeq << 20)))
	stagger := time.Duration(staggerRng.Int63n(int64(connectStaggerMax)))
	select {
	case <-time.After(stagger):
	case <-ctx.Done():
		return meshStats{}, ctx.Err()
	}

	targets := make([]rosterEntry, 0, len(roster))
	for _, e := range roster {
		if e.Seq > selfSeq {
			targets = append(targets, e)
		}
	}

	start := time.Now()
	var attempts atomic.Int64
	edgeErrs := make([][]error, len(targets))
	var g errgroup.Group
	g.SetLimit(connectParallel)
	for i, e := range targets {
		g.Go(func() error {
			rng := rand.New(rand.NewSource(int64(p.Seed) ^ connectSeedSalt ^ (selfSeq << 20) ^ (e.Seq << 40)))
			edgeErrs[i] = connectPeer(ctx, nd, runenv, e, rng, &attempts)
			return nil
		})
	}
	_ = g.Wait()

	stats := meshStats{Targets: len(targets), FailedSeqs: []int64{}}
	failLines := make([]string, 0, len(targets))
	for i, errs := range edgeErrs {
		if len(errs) == 0 {
			stats.Connected++
			continue
		}
		stats.FailedSeqs = append(stats.FailedSeqs, targets[i].Seq)
		failLines = append(failLines, fmt.Sprintf("seq %d: %s", targets[i].Seq, classifyAttempts(errs)))
	}
	need := int(math.Ceil(p.MeshQuorum * float64(stats.Targets)))
	runenv.RecordMessage("mesh bootstrap: %d/%d peers (quorum %.2f, need %d), %d attempts total, elapsed %s, stagger %s, failed edges: %v",
		stats.Connected, stats.Targets, p.MeshQuorum, need, attempts.Load(),
		time.Since(start).Round(time.Millisecond), stagger.Round(time.Millisecond), stats.FailedSeqs)
	if stats.Connected < need {
		return stats, fmt.Errorf("mesh below quorum: %d of %d peers connected, need %d (quorum %.2f): %s",
			stats.Connected, stats.Targets, need, p.MeshQuorum, strings.Join(failLines, "; "))
	}
	return stats, nil
}

// connectPeer runs one peer's attempt loop; an empty slice means the edge
// connected. The backoff clear before every dial is required: concurrent
// activity leaves swarm dial-backoff entries that fail even attempt one
func connectPeer(ctx context.Context, nd *node, runenv *runtime.RunEnv, e rosterEntry, rng *rand.Rand, attempts *atomic.Int64) []error {
	deadline := time.Now().Add(connectPeerBudget)
	errs := make([]error, 0, connectAttempts)
loop:
	for attempt := 1; attempt <= connectAttempts; attempt++ {
		attempts.Add(1)
		if sw, ok := nd.host.Network().(*swarm.Swarm); ok {
			sw.Backoff().Clear(e.Info.ID)
		}
		cctx, cancel := context.WithTimeout(ctx, connectTimeout)
		err := nd.host.Connect(cctx, e.Info)
		cancel()
		if err == nil {
			return nil
		}
		errs = append(errs, err)
		if attempt == connectAttempts || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		wait := connectBackoffBase << (attempt - 1)
		wait += time.Duration(rng.Int63n(int64(wait) / 2))
		if remaining := time.Until(deadline); wait > remaining {
			wait = remaining
		}
		runenv.RecordMessage("connect to seq %d attempt %d/%d failed (%s), retrying in %s",
			e.Seq, attempt, connectAttempts, err, wait.Round(time.Millisecond))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			break loop
		}
	}
	runenv.RecordMessage("seq %d: %s", e.Seq, classifyAttempts(errs))
	return errs
}

// classifyAttempts folds a peer's dial errors into one log line, which
// separates a stale backoff cache from a saturated link
func classifyAttempts(errs []error) string {
	counts := make(map[string]int, len(errs))
	order := make([]string, 0, len(errs))
	for _, err := range errs {
		s := err.Error()
		var class string
		switch {
		case strings.Contains(s, "backoff"):
			class = "dial backoff"
		case strings.Contains(s, "timeout"):
			class = "i/o timeout"
		case strings.Contains(s, "reset"):
			class = "connection reset"
		case strings.Contains(s, "refused"):
			class = "connection refused"
		case strings.Contains(s, "canceled"):
			class = "canceled"
		default:
			class = "other"
		}
		if counts[class] == 0 {
			order = append(order, class)
		}
		counts[class]++
	}
	parts := make([]string, len(order))
	for i, c := range order {
		parts[i] = fmt.Sprintf("%dx %s", counts[c], c)
	}
	return fmt.Sprintf("%d attempts (%s)", len(errs), strings.Join(parts, ", "))
}

// waitRoutingTable waits for three stable 1 s samples or the deadline. No
// size target: at N=50 the table saturates below N-1 on bucket capacity
func waitRoutingTable(nd *node, runenv *runtime.RunEnv) {
	deadline := time.Now().Add(rtDeadline)
	last, stable := -1, 0
	for time.Now().Before(deadline) {
		size := nd.dht.RoutingTable().Size()
		if size == last {
			stable++
			if stable >= 3 && size > 0 {
				break
			}
		} else {
			last, stable = size, 0
		}
		time.Sleep(time.Second)
	}
	runenv.RecordMessage("routing table settled at %d entries", nd.dht.RoutingTable().Size())
}

// deriveBlocks derives the run's blocks and CIDs from the seed alone, so
// provider and initiator agree without a sync topic. 4096 B keeps the
// bitswap server answering WANT-HAVE with a HAVE instead of the block
// (DefaultWantHaveReplaceSize = 1024 B)
func deriveBlocks(seed, n int) ([]blocks.Block, []cid.Cid, error) {
	const blockSize = 4096
	blks := make([]blocks.Block, n)
	cids := make([]cid.Cid, n)
	for i := range blks {
		data := make([]byte, 0, blockSize+sha256.Size)
		for chunk := 0; len(data) < blockSize; chunk++ {
			h := sha256.Sum256(fmt.Appendf(nil, "hiddenpath/seed=%d/job=%d/chunk=%d", seed, i, chunk))
			data = append(data, h[:]...)
		}
		data = data[:blockSize]
		mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
		if err != nil {
			return nil, nil, fmt.Errorf("hashing block %d: %w", i, err)
		}
		cids[i] = cid.NewCidV1(cid.Raw, mh)
		b, err := blocks.NewBlockWithCid(data, cids[i])
		if err != nil {
			return nil, nil, fmt.Errorf("building block %d: %w", i, err)
		}
		blks[i] = b
	}
	return blks, cids, nil
}

// provideAll announces every CID. Failures fail the run: a missing record
// turns that job into a guaranteed miss and contaminates the latency series
func provideAll(ctx context.Context, nd *node, runenv *runtime.RunEnv, cids []cid.Cid) error {
	start := time.Now()
	for i, c := range cids {
		pctx, cancel := context.WithTimeout(ctx, provideTimeout)
		err := nd.dht.Provide(pctx, c, true)
		cancel()
		if err != nil {
			return fmt.Errorf("announcing cid %d (%s): %w", i, c, err)
		}
	}
	runenv.RecordMessage("provider announced %d cids in %s", len(cids), time.Since(start).Round(time.Millisecond))
	return nil
}

// waitPoolFull blocks until the relay pool holds every serving instance.
// The key exchange retries a missing record only at its TTL/8 refresh,
// outside the measurement window, so after poolBounceAfter without
// progress the node bounces its outbound connections to re-fire onConnected
func waitPoolFull(ctx context.Context, nd *node, runenv *runtime.RunEnv, roster []rosterEntry, selfSeq int64, want int) error {
	deadline := time.Now().Add(poolDeadline)
	lastProgress := time.Now()
	lastLive := -1
	for {
		live, need := nd.svc.PoolStatus()
		if live >= want {
			runenv.RecordMessage("pool full: %d live (need %d for one job)", live, need)
			return nil
		}
		if live != lastLive {
			lastLive = live
			lastProgress = time.Now()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pool incomplete after %s: %d of %d live", poolDeadline, live, want)
		}
		if time.Since(lastProgress) > poolBounceAfter {
			runenv.RecordMessage("pool stalled at %d/%d, bouncing outbound connections", live, want)
			for _, e := range roster {
				if e.Seq <= selfSeq {
					continue
				}
				_ = nd.host.Network().ClosePeer(e.Info.ID)
				cctx, cancel := context.WithTimeout(ctx, connectTimeout)
				_ = nd.host.Connect(cctx, e.Info)
				cancel()
			}
			lastProgress = time.Now()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// victimSet draws the f victims from the server pool seqs, excluding
// initiator, provider and clients. Every instance computes the same set
func victimSet(seed, n, f int, clients map[int64]bool) map[int64]bool {
	var candidates []int64
	for s := int64(3); s <= int64(n); s++ {
		if !clients[s] {
			candidates = append(candidates, s)
		}
	}
	rng := rand.New(rand.NewSource(int64(seed) ^ victimSeedSalt))
	perm := rng.Perm(len(candidates))
	victims := make(map[int64]bool, f)
	for i := 0; i < f; i++ {
		victims[candidates[perm[i]]] = true
	}
	return victims
}

// rosterVictims maps victim and provider seqs to peer IDs; the analysis
// joins the exported branch paths against these strings
func rosterVictims(roster []rosterEntry, victims map[int64]bool) (victimPeers []string, providerPeer string) {
	victimPeers = []string{}
	for _, e := range roster {
		if victims[e.Seq] {
			victimPeers = append(victimPeers, e.Info.ID.String())
		}
		if roleOf(e.Seq) == "provider" {
			providerPeer = e.Info.ID.String()
		}
	}
	return victimPeers, providerPeer
}

// clientSet returns the seqs that run as DHT clients: the initiator first,
// then pool nodes from the highest seq down; the provider never converts
func clientSet(n, clients int) map[int64]bool {
	set := make(map[int64]bool, clients)
	if clients > 0 {
		set[1] = true
	}
	for s := int64(n); len(set) < clients && s >= 3; s-- {
		set[s] = true
	}
	return set
}

func roleOf(seq int64) string {
	switch seq {
	case 1:
		return "initiator"
	case 2:
		return "provider"
	default:
		return "pool"
	}
}
