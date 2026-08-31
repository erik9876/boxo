package main

import (
	"fmt"
	"time"

	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
	"github.com/testground/sdk-go/runtime"
)

// one run's knobs, read once from the composition; N is the instance count
type params struct {
	Mode           string
	L              int // MinProxyCPL
	K              int // branches per job
	M              int // SURBs per branch
	F              int // kill count
	Timeout        time.Duration
	ProxyTimeout   time.Duration
	Retransmit     bool
	DrawPolicy     string            // param string, exported to the result
	Draw           sphinx.DrawPolicy // derived from DrawPolicy
	ProviderBlocks bool
	Seed           int
	Jobs           int
	Clients        int
	MeshQuorum     float64
	Latency        time.Duration
	Bandwidth      uint64
	TimeoutMs      int
	ProxyTimeoutMs int
	LatencyMs      int
	EdgeEpsilon    float64
}

func (p params) sphinx() bool { return p.Mode == "sphinx" }

func parseParams(runenv *runtime.RunEnv) (params, error) {
	p := params{
		Mode:           stringParam(runenv, "mode", "sphinx"),
		L:              intParam(runenv, "l", 0),
		K:              intParam(runenv, "k", 2),
		M:              intParam(runenv, "m", 3),
		F:              intParam(runenv, "f", 0),
		TimeoutMs:      intParam(runenv, "timeout_ms", 30000),
		ProxyTimeoutMs: intParam(runenv, "proxy_timeout_ms", 10000),
		Retransmit:     boolParam(runenv, "retransmit", true),
		DrawPolicy:     stringParam(runenv, "draw_policy", "exclusive"),
		ProviderBlocks: boolParam(runenv, "provider_blocks", false),
		Seed:           intParam(runenv, "seed", 1),
		Jobs:           intParam(runenv, "jobs", 20),
		Clients:        intParam(runenv, "clients", 0),
		MeshQuorum:     floatParam(runenv, "mesh_quorum", 0.9),
		LatencyMs:      intParam(runenv, "latency_ms", 100),
		Bandwidth:      uint64(intParam(runenv, "bandwidth", 1048576)),
		// -1 disables the bias: on a full mesh an unconnected relay is
		// exactly a post-kill victim, so 0.0 would prefer victims
		EdgeEpsilon: floatParam(runenv, "edge_epsilon", -1.0),
	}
	p.Timeout = time.Duration(p.TimeoutMs) * time.Millisecond
	p.ProxyTimeout = time.Duration(p.ProxyTimeoutMs) * time.Millisecond
	p.Latency = time.Duration(p.LatencyMs) * time.Millisecond

	n := runenv.TestInstanceCount
	if p.Mode != "vanilla" && p.Mode != "sphinx" {
		return p, fmt.Errorf("mode must be vanilla or sphinx, got %q", p.Mode)
	}
	if n < 5 {
		return p, fmt.Errorf("need at least 5 instances, got %d", n)
	}
	if p.Clients < 0 || p.Clients > n-1 {
		return p, fmt.Errorf("clients = %d out of range 0..%d (the provider stays a server)", p.Clients, n-1)
	}
	poolClients := 0
	if p.Clients > 1 {
		poolClients = p.Clients - 1
	}
	serverPool := n - 2 - poolClients
	if p.F < 0 || p.F > serverPool-2 {
		// keep at least two server pool nodes alive besides the endpoints
		return p, fmt.Errorf("f = %d out of range for %d instances with %d clients (max %d)", p.F, n, p.Clients, serverPool-2)
	}
	if p.L < 0 || p.L > 256 {
		return p, fmt.Errorf("l = %d out of range 0..256", p.L)
	}
	if p.K < 1 || p.M < 1 || p.Jobs < 1 || p.Seed < 0 {
		return p, fmt.Errorf("k, m, jobs must be >= 1 and seed >= 0 (got k=%d m=%d jobs=%d seed=%d)", p.K, p.M, p.Jobs, p.Seed)
	}
	if p.Timeout <= 0 || p.ProxyTimeout <= 0 {
		return p, fmt.Errorf("timeouts must be positive")
	}
	if p.MeshQuorum <= 0 || p.MeshQuorum > 1 {
		return p, fmt.Errorf("mesh_quorum = %v out of range (0, 1]", p.MeshQuorum)
	}
	if p.sphinx() {
		need := p.K * (sphinx.NrHops + 2*p.M)
		initiatorPool := n - p.Clients
		if p.Clients == 0 {
			initiatorPool--
		}
		if initiatorPool < need {
			return p, fmt.Errorf("clients = %d leaves the initiator a pool of %d, one job needs %d", p.Clients, initiatorPool, need)
		}
	}
	if p.EdgeEpsilon > 1 {
		return p, fmt.Errorf("edge_epsilon = %v above 1 (negative disables the bias)", p.EdgeEpsilon)
	}
	var err error
	if p.Draw, err = parseDrawPolicy(p.DrawPolicy); err != nil {
		return p, err
	}
	if err := validateDrawEpsilon(p.Draw, p.EdgeEpsilon); err != nil {
		return p, err
	}
	return p, nil
}

// validateDrawEpsilon mirrors the JobConfig rejection so a run fails at
// param parsing instead of inside service construction on every instance
func validateDrawEpsilon(draw sphinx.DrawPolicy, eps float64) error {
	if draw == sphinx.DrawIndependent && eps >= 0 {
		return fmt.Errorf("edge_epsilon = %v cannot combine with draw_policy=independent", eps)
	}
	return nil
}

// parseDrawPolicy maps the draw_policy param onto the JobConfig policy
func parseDrawPolicy(s string) (sphinx.DrawPolicy, error) {
	switch s {
	case "exclusive":
		return sphinx.DrawExclusive, nil
	case "per-attempt":
		return sphinx.DrawPerAttempt, nil
	case "independent":
		return sphinx.DrawIndependent, nil
	}
	return 0, fmt.Errorf("draw_policy must be exclusive, per-attempt or independent, got %q", s)
}

func stringParam(runenv *runtime.RunEnv, key, def string) string {
	if !runenv.IsParamSet(key) {
		return def
	}
	return runenv.StringParam(key)
}

func intParam(runenv *runtime.RunEnv, key string, def int) int {
	if !runenv.IsParamSet(key) {
		return def
	}
	return runenv.IntParam(key)
}

func boolParam(runenv *runtime.RunEnv, key string, def bool) bool {
	if !runenv.IsParamSet(key) {
		return def
	}
	return runenv.BooleanParam(key)
}

func floatParam(runenv *runtime.RunEnv, key string, def float64) float64 {
	if !runenv.IsParamSet(key) {
		return def
	}
	return runenv.FloatParam(key)
}
