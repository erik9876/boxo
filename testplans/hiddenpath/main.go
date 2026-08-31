// Command hiddenpath is the Testground harness for the thesis evaluation.
// One testcase covers calibration and both measurement series through
// parameters; compositions/gen.py emits the per-cell compositions
package main

import (
	"context"
	"fmt"
	"os"
	goruntime "runtime"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/testground/sdk-go/network"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

func main() {
	// every co-located instance sees the host's full core count; two Ps
	// per instance bound the fleet at ~2x oversubscription
	if os.Getenv("GOMAXPROCS") == "" {
		goruntime.GOMAXPROCS(2)
	}
	run.InvokeMap(map[string]interface{}{
		"discovery": run.InitializedTestCaseFn(runDiscovery),
	})
}

func runDiscovery(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	p, err := parseParams(runenv)
	if err != nil {
		return err
	}
	ctx := context.Background()
	client := initCtx.SyncClient
	n := runenv.TestInstanceCount
	seq := initCtx.GlobalSeq
	role := roleOf(seq)
	clientSeqs := clientSet(n, p.Clients)
	isClient := clientSeqs[seq]
	victims := victimSet(p.Seed, n, p.F, clientSeqs)
	isVictim := role == "pool" && victims[seq]
	survivors := n - p.F

	runenv.RecordMessage("instance seq=%d role=%s mode=%s client=%t victim=%t (N=%d f=%d clients=%d seed=%d)",
		seq, role, p.Mode, isClient, isVictim, n, p.F, p.Clients, p.Seed)

	// one attempt must outlast the proxy's discovery plus the round trips
	if p.sphinx() && p.Timeout < p.ProxyTimeout+8*p.Latency {
		runenv.RecordMessage("WARNING: timeout %s < proxy timeout %s + 8x latency; attempts may expire on live paths",
			p.Timeout, p.ProxyTimeout)
	}

	// link shaping; skipped by the SDK when no sidecar runs (local:exec
	// smoke). Deadline-bounded so a wedged sync stream fails by name
	netCtx, netCancel := context.WithTimeout(ctx, barrierCap)
	defer netCancel()
	initCtx.NetClient.MustWaitNetworkInitialized(netCtx)
	initCtx.NetClient.MustConfigureNetwork(netCtx, &network.Config{
		Network: "default",
		Enable:  true,
		Default: network.LinkShape{
			Latency:   p.Latency,
			Bandwidth: p.Bandwidth,
		},
		CallbackState:  sync.State("network-configured"),
		CallbackTarget: n,
		RoutingPolicy:  network.DenyAll,
	})

	// stack before any connection exists (see buildNode)
	evlog := newEventLog()
	nd, err := buildNode(ctx, p, isClient, evlog)
	if err != nil {
		return err
	}
	closeNode := true
	defer func() {
		if closeNode {
			nd.close()
		}
	}()

	self := rosterEntry{Seq: seq, Info: hostInfo(nd)}
	roster, err := publishAndCollectRoster(ctx, runenv, client, self)
	if err != nil {
		return err
	}
	mesh, err := connectMesh(ctx, nd, runenv, roster, seq, p)
	if err != nil {
		return err
	}
	if err := barrier(ctx, client, stateMeshReady, n, barrierCap); err != nil {
		return err
	}

	if err := nd.dht.Bootstrap(ctx); err != nil {
		runenv.RecordMessage("dht bootstrap: %s", err)
	}
	waitRoutingTable(nd, runenv)
	if err := barrier(ctx, client, stateDHTReady, n, barrierCap); err != nil {
		return err
	}

	blks, cids, err := deriveBlocks(p.Seed, p.Jobs)
	if err != nil {
		return err
	}
	if role == "provider" {
		if p.ProviderBlocks {
			if err := nd.bstore.PutMany(ctx, blks); err != nil {
				return fmt.Errorf("storing provider blocks: %w", err)
			}
			runenv.RecordMessage("provider stored %d blocks (broadcast-hit cell)", len(blks))
		}
		if err := provideAll(ctx, nd, runenv, cids); err != nil {
			return err
		}
	}
	if err := barrier(ctx, client, stateProvided, n, barrierCap); err != nil {
		return err
	}

	// the churn model rests on a full pool: dead peers stay sampleable
	if p.sphinx() {
		want := n - p.Clients
		if !isClient {
			want--
		}
		if err := waitPoolFull(ctx, nd, runenv, roster, seq, want); err != nil {
			return err
		}
	}
	if err := barrier(ctx, client, statePoolsFull, n, barrierCap); err != nil {
		return err
	}

	// kill window: victims write a truncated result and die hard; os.Exit
	// runs no defers, so the network sees a crash
	if err := barrier(ctx, client, statePreKill, n, barrierCap); err != nil {
		return err
	}
	if isVictim {
		r := buildResult(runenv, p, nd, seq, role, true, mesh, nil, nil)
		if err := writeResult(runenv, r); err != nil {
			runenv.RecordMessage("victim result write failed: %s", err)
		}
		runenv.RecordSuccess()
		os.Exit(0)
	}
	time.Sleep(postKillSettle)
	if err := barrier(ctx, client, statePostKill, survivors, barrierCap); err != nil {
		return err
	}

	// everyone but the initiator parks on the barrier, whose cap covers
	// every job sitting out its budget
	jobsDoneCap := time.Duration(p.Jobs)*nd.jobBudget(p) + 2*time.Minute
	_, _, bwPreJobs := bwSnapshot(nd)
	var jobResults []jobResult
	if role == "initiator" {
		jobResults = runJobs(ctx, nd, runenv, evlog, cids, nd.jobBudget(p))
	}
	if err := barrier(ctx, client, stateJobsDone, survivors, jobsDoneCap); err != nil {
		return err
	}

	// go-flow-metrics moves marks into totals on a ~1 s sweeper; without
	// this pause the trailing job traffic is missing
	time.Sleep(bwFlushWait)

	r := buildResult(runenv, p, nd, seq, role, false, mesh, jobResults, bwPreJobs)
	r.ProbeEvents = evlog.probeSnapshot()
	r.WalkEvents = evlog.walkSnapshot()
	if role == "initiator" {
		r.VictimPeers, r.ProviderPeer = rosterVictims(roster, victims)
	}
	if r.OverBudget {
		runenv.RecordMessage("WARNING: measurement window %.0f ms exceeded the 7-minute refresh-free budget", r.WindowMs)
	}
	if err := writeResult(runenv, r); err != nil {
		return err
	}
	if err := barrier(ctx, client, stateDone, survivors, barrierCap); err != nil {
		return err
	}

	closeNode = false
	nd.close()
	runenv.RecordSuccess()
	return nil
}

// bwPreJobs may be nil (victims); the job-window delta is then zero
func buildResult(runenv *runtime.RunEnv, p params, nd *node, seq int64, role string, victim bool, mesh meshStats, jobs []jobResult, bwPreJobs map[string]bwStat) *result {
	window := time.Since(nd.windowStart)
	in, out, byProto := bwSnapshot(nd)
	r := &result{
		RunID:  runenv.TestRun,
		Seq:    seq,
		Role:   role,
		PeerID: nd.host.ID().String(),
		Victim: victim,
		Client: nd.isClient,

		Mode:           p.Mode,
		N:              runenv.TestInstanceCount,
		L:              p.L,
		K:              p.K,
		M:              p.M,
		F:              p.F,
		TimeoutMs:      p.TimeoutMs,
		ProxyTimeoutMs: p.ProxyTimeoutMs,
		Retransmit:     p.Retransmit,
		DrawPolicy:     p.DrawPolicy,
		ProviderBlocks: p.ProviderBlocks,
		Seed:           p.Seed,
		Jobs:           p.Jobs,
		Clients:        p.Clients,
		MeshQuorum:     p.MeshQuorum,
		LatencyMs:      p.LatencyMs,
		Bandwidth:      p.Bandwidth,
		EdgeEpsilon:    p.EdgeEpsilon,

		MeshTargets:    mesh.Targets,
		MeshConnected:  mesh.Connected,
		FailedEdgeSeqs: mesh.FailedSeqs,

		WindowMs:   float64(window.Microseconds()) / 1000.0,
		OverBudget: window > measurementCap,

		BwTotalIn:    in,
		BwTotalOut:   out,
		BwByProtocol: byProto,

		JobResults: jobs,
	}
	if nd.svc != nil {
		live, need := nd.svc.PoolStatus()
		r.PoolLive, r.PoolNeed = live, need
		jm := nd.svc.JobMetrics()
		pm := nd.svc.ProxyMetrics()
		tm := nd.svc.TransportMetrics()
		r.JobMetrics, r.ProxyMetrics, r.TransportMetrics = &jm, &pm, &tm
		r.SphinxMode = nd.svc.Mode().String()
	}
	if bwPreJobs != nil {
		delta := bwDelta(byProto, bwPreJobs)
		r.BwJobsByProtocol = delta
		for _, st := range delta {
			r.BwJobsIn += st.In
			r.BwJobsOut += st.Out
		}
	}
	return r
}

func hostInfo(nd *node) peer.AddrInfo {
	return peer.AddrInfo{ID: nd.host.ID(), Addrs: nd.host.Addrs()}
}
