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
	// RepliesWon settled their job, RepliesDuplicate missed the job table,
	// RepliesInvalid were undecodable and dropped only their SURB
	RepliesWon       atomic.Uint64
	RepliesDuplicate atomic.Uint64
	RepliesInvalid   atomic.Uint64
	Retransmits      atomic.Uint64
	// no second wave, job waited out the final timer
	RetransmitsFailed atomic.Uint64
	// ProxyCPLSum/ProxyCPLCount = achieved proxy proximity to the CID's
	// provider key, recorded at every attempt (ℓ = 0 included)
	ProxyCPLSum   atomic.Uint64
	ProxyCPLCount atomic.Uint64
	JobsSucceeded atomic.Uint64
	JobsFailed    atomic.Uint64
	JobsTimedOut  atomic.Uint64
	JobsCanceled  atomic.Uint64
}

type JobMetricsSnapshot struct {
	JobsStarted       uint64
	BranchPacketsSent uint64
	SendFailures      uint64
	RepliesWon        uint64
	RepliesDuplicate  uint64
	RepliesInvalid    uint64
	Retransmits       uint64
	RetransmitsFailed uint64
	ProxyCPLSum       uint64
	ProxyCPLCount     uint64
	JobsSucceeded     uint64
	JobsFailed        uint64
	JobsTimedOut      uint64
	JobsCanceled      uint64
}

func (m *JobMetrics) Snapshot() JobMetricsSnapshot {
	return JobMetricsSnapshot{
		JobsStarted:       m.JobsStarted.Load(),
		BranchPacketsSent: m.BranchPacketsSent.Load(),
		SendFailures:      m.SendFailures.Load(),
		RepliesWon:        m.RepliesWon.Load(),
		RepliesDuplicate:  m.RepliesDuplicate.Load(),
		RepliesInvalid:    m.RepliesInvalid.Load(),
		Retransmits:       m.Retransmits.Load(),
		RetransmitsFailed: m.RetransmitsFailed.Load(),
		ProxyCPLSum:       m.ProxyCPLSum.Load(),
		ProxyCPLCount:     m.ProxyCPLCount.Load(),
		JobsSucceeded:     m.JobsSucceeded.Load(),
		JobsFailed:        m.JobsFailed.Load(),
		JobsTimedOut:      m.JobsTimedOut.Load(),
		JobsCanceled:      m.JobsCanceled.Load(),
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
