package sphinx

import "sync/atomic"

// JobMetrics counts initiator-side events for the evaluation. Fields are
// individually atomic; a snapshot is not a consistent cut, which is fine
// for settled totals. Once traffic quiesces:
//
//	JobsSucceeded + JobsFailed + JobsTimedOut + JobsCanceled == JobsStarted
type JobMetrics struct {
	// registered and at least one branch sent
	JobsStarted       atomic.Uint64
	BranchPacketsSent atomic.Uint64
	SendFailures      atomic.Uint64
	// RepliesCollected were accepted as a branch's OK answer,
	// RepliesFailed as its Failed answer; RepliesDuplicate missed the job
	// table or their already-answered branch; RepliesInvalid were
	// undecodable and retired their branch without a contribution
	RepliesCollected atomic.Uint64
	RepliesFailed    atomic.Uint64
	RepliesDuplicate atomic.Uint64
	RepliesInvalid   atomic.Uint64
	Retransmits      atomic.Uint64
	// no second wave, job waited out the final timer
	RetransmitsFailed atomic.Uint64
	// ProxyCPLSum/ProxyCPLCount = achieved proxy proximity to the CID's
	// provider key, recorded at every attempt (ℓ = 0 included)
	ProxyCPLSum   atomic.Uint64
	ProxyCPLCount atomic.Uint64
	// initiator-edge bias diagnostics, recorded per attempt when a
	// PeerState is configured (bias disabled included). FirstHop* cover
	// the forward first hop (k per attempt), SurbLast* the last relay of
	// each return path (k·m per attempt); the addr counters are the c_I
	// diagnostic on the first hop only, an observation that never steers
	// selection
	FirstHopConnected    atomic.Uint64
	FirstHopUnconnected  atomic.Uint64
	FirstHopWithAddrs    atomic.Uint64
	FirstHopWithoutAddrs atomic.Uint64
	SurbLastConnected    atomic.Uint64
	SurbLastUnconnected  atomic.Uint64
	JobsSucceeded        atomic.Uint64
	JobsFailed    atomic.Uint64
	JobsTimedOut  atomic.Uint64
	JobsCanceled  atomic.Uint64
}

type JobMetricsSnapshot struct {
	JobsStarted       uint64
	BranchPacketsSent uint64
	SendFailures      uint64
	RepliesCollected  uint64
	RepliesFailed     uint64
	RepliesDuplicate  uint64
	RepliesInvalid    uint64
	Retransmits       uint64
	RetransmitsFailed uint64
	ProxyCPLSum          uint64
	ProxyCPLCount        uint64
	FirstHopConnected    uint64
	FirstHopUnconnected  uint64
	FirstHopWithAddrs    uint64
	FirstHopWithoutAddrs uint64
	SurbLastConnected    uint64
	SurbLastUnconnected  uint64
	JobsSucceeded        uint64
	JobsFailed           uint64
	JobsTimedOut         uint64
	JobsCanceled         uint64
}

func (m *JobMetrics) Snapshot() JobMetricsSnapshot {
	return JobMetricsSnapshot{
		JobsStarted:       m.JobsStarted.Load(),
		BranchPacketsSent: m.BranchPacketsSent.Load(),
		SendFailures:      m.SendFailures.Load(),
		RepliesCollected:  m.RepliesCollected.Load(),
		RepliesFailed:     m.RepliesFailed.Load(),
		RepliesDuplicate:  m.RepliesDuplicate.Load(),
		RepliesInvalid:    m.RepliesInvalid.Load(),
		Retransmits:       m.Retransmits.Load(),
		RetransmitsFailed: m.RetransmitsFailed.Load(),
		ProxyCPLSum:          m.ProxyCPLSum.Load(),
		ProxyCPLCount:        m.ProxyCPLCount.Load(),
		FirstHopConnected:    m.FirstHopConnected.Load(),
		FirstHopUnconnected:  m.FirstHopUnconnected.Load(),
		FirstHopWithAddrs:    m.FirstHopWithAddrs.Load(),
		FirstHopWithoutAddrs: m.FirstHopWithoutAddrs.Load(),
		SurbLastConnected:    m.SurbLastConnected.Load(),
		SurbLastUnconnected:  m.SurbLastUnconnected.Load(),
		JobsSucceeded:        m.JobsSucceeded.Load(),
		JobsFailed:           m.JobsFailed.Load(),
		JobsTimedOut:         m.JobsTimedOut.Load(),
		JobsCanceled:         m.JobsCanceled.Load(),
	}
}

// TransportMetrics counts the pre-send next-hop lookups. With a finder
// configured, Lookups tracks SendPacket calls one to one (delivered or
// not), which is exactly the uniformity claim the evaluation checks
type TransportMetrics struct {
	Lookups        atomic.Uint64
	LookupFailures atomic.Uint64
}

type TransportMetricsSnapshot struct {
	Lookups        uint64
	LookupFailures uint64
}

func (m *TransportMetrics) Snapshot() TransportMetricsSnapshot {
	return TransportMetricsSnapshot{
		Lookups:        m.Lookups.Load(),
		LookupFailures: m.LookupFailures.Load(),
	}
}

type ProxyMetrics struct {
	// admitted and ran; drops don't count
	JobsExecuted atomic.Uint64
}

type ProxyMetricsSnapshot struct {
	JobsExecuted uint64
}

func (m *ProxyMetrics) Snapshot() ProxyMetricsSnapshot {
	return ProxyMetricsSnapshot{JobsExecuted: m.JobsExecuted.Load()}
}
