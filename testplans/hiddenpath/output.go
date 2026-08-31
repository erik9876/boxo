package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
	"github.com/testground/sdk-go/runtime"
)

// stream payload bytes, without TCP/Noise/yamux framing
type bwStat struct {
	In  int64 `json:"in"`
	Out int64 `json:"out"`
}

// result is the one JSON document per instance; victims write it truncated
// before their hard exit
type result struct {
	RunID  string `json:"run_id"`
	Seq    int64  `json:"seq"`
	Role   string `json:"role"`
	PeerID string `json:"peer_id"`
	Victim bool   `json:"victim"`

	Mode           string  `json:"mode"`
	N              int     `json:"n"`
	L              int     `json:"l"`
	K              int     `json:"k"`
	M              int     `json:"m"`
	F              int     `json:"f"`
	TimeoutMs      int     `json:"timeout_ms"`
	ProxyTimeoutMs int     `json:"proxy_timeout_ms"`
	Retransmit     bool    `json:"retransmit"`
	DrawPolicy     string  `json:"draw_policy"`
	ProviderBlocks bool    `json:"provider_blocks"`
	Seed           int     `json:"seed"`
	Jobs           int     `json:"jobs"`
	Clients        int     `json:"clients"`
	Client         bool    `json:"client"`
	MeshQuorum     float64 `json:"mesh_quorum"`
	LatencyMs      int     `json:"latency_ms"`
	Bandwidth      uint64  `json:"bandwidth"`
	EdgeEpsilon    float64 `json:"edge_epsilon"`

	// edges this node owned (higher seqs) and which never came up
	MeshTargets    int     `json:"mesh_targets"`
	MeshConnected  int     `json:"mesh_connected"`
	FailedEdgeSeqs []int64 `json:"failed_edge_seqs"`

	// from sphinx service construction to snapshot; OverBudget flags a
	// breach of the refresh-free budget
	WindowMs   float64 `json:"window_ms"`
	OverBudget bool    `json:"over_budget"`

	PoolLive int `json:"pool_live"`
	PoolNeed int `json:"pool_need"`

	SphinxMode string `json:"sphinx_mode,omitempty"`

	JobMetrics       *sphinx.JobMetricsSnapshot       `json:"job_metrics,omitempty"`
	ProxyMetrics     *sphinx.ProxyMetricsSnapshot     `json:"proxy_metrics,omitempty"`
	TransportMetrics *sphinx.TransportMetricsSnapshot `json:"transport_metrics,omitempty"`

	// cumulative host-lifetime bytes plus the job-loop delta
	BwTotalIn        int64             `json:"bw_total_in"`
	BwTotalOut       int64             `json:"bw_total_out"`
	BwByProtocol     map[string]bwStat `json:"bw_by_protocol"`
	BwJobsIn         int64             `json:"bw_jobs_in"`
	BwJobsOut        int64             `json:"bw_jobs_out"`
	BwJobsByProtocol map[string]bwStat `json:"bw_jobs_by_protocol"`

	JobResults []jobResult `json:"job_results,omitempty"`

	// initiator only: the run's victims and the provider by peer ID
	VictimPeers  []string `json:"victim_peers,omitempty"`
	ProviderPeer string   `json:"provider_peer,omitempty"`

	// every completed Probe call and route-stage DHT walk this instance ran
	ProbeEvents []probeEvent `json:"probe_events,omitempty"`
	WalkEvents  []walkEvent  `json:"walk_events,omitempty"`
}

func bwSnapshot(nd *node) (in, out int64, byProto map[string]bwStat) {
	totals := nd.bwc.GetBandwidthTotals()
	byProto = make(map[string]bwStat)
	for proto, st := range nd.bwc.GetBandwidthByProtocol() {
		byProto[string(proto)] = bwStat{In: st.TotalIn, Out: st.TotalOut}
	}
	return totals.TotalIn, totals.TotalOut, byProto
}

func bwDelta(later, earlier map[string]bwStat) map[string]bwStat {
	delta := make(map[string]bwStat, len(later))
	for proto, l := range later {
		e := earlier[proto]
		delta[proto] = bwStat{In: l.In - e.In, Out: l.Out - e.Out}
	}
	return delta
}

// one output directory per instance; `testground collect` bundles them
func writeResult(runenv *runtime.RunEnv, r *result) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling result: %w", err)
	}
	path := filepath.Join(runenv.TestOutputsPath, "result.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	runenv.RecordMessage("result written to %s", path)
	return nil
}
