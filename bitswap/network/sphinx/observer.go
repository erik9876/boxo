package sphinx

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// BranchPath is one branch of an attempt as drawn: the forward hops with
// the proxy last, and per return path its relays; the terminal hop of a
// return path, the initiator itself, is implicit
type BranchPath struct {
	Forward []peer.ID
	Returns [][]peer.ID
}

// BranchOutcome is a branch's first counted answer, or how it died
type BranchOutcome int

const (
	// a reply with ReplyStatusOK counted for the branch
	BranchOK BranchOutcome = iota
	// a reply with ReplyStatusFailed counted
	BranchFailed
	// the forward packet never left; the initiator knows at send time
	BranchDeadSend
	// every reply envelope was spent on undecodable payloads
	BranchExhausted
)

func (o BranchOutcome) String() string {
	switch o {
	case BranchOK:
		return "ok"
	case BranchFailed:
		return "failed"
	case BranchDeadSend:
		return "dead_send"
	case BranchExhausted:
		return "exhausted"
	}
	return "unknown"
}

// SettleKind labels how a job ended
type SettleKind int

const (
	SettleSuccess SettleKind = iota
	SettleFailed
	SettleInvalidReplies
	SettleSendFailed
	SettleTimeout
	SettlePoolShortfall
	SettleClosed
	SettleOther
)

func (k SettleKind) String() string {
	switch k {
	case SettleSuccess:
		return "success"
	case SettleFailed:
		return "failed"
	case SettleInvalidReplies:
		return "invalid_replies"
	case SettleSendFailed:
		return "send_failed"
	case SettleTimeout:
		return "timeout"
	case SettlePoolShortfall:
		return "pool_shortfall"
	case SettleClosed:
		return "closed"
	}
	return "other"
}

// JobObserver receives initiator-side job events; an evaluation seam like
// JobConfig.DisableRetransmit, nil disables it. Calls run outside the
// manager's lock, so an implementation may call back into the manager.
// Ordering: OnAttempt precedes every outcome of its attempt, and a job's
// OnSettled fires before its JobResult is delivered. Branches that stay
// silent produce no OnBranchOutcome; the observer infers them at settle
type JobObserver interface {
	OnAttempt(jobID uint64, c cid.Cid, attempt int, paths []BranchPath)
	OnBranchOutcome(jobID uint64, attempt, branch int, outcome BranchOutcome, at time.Time)
	OnSettled(jobID uint64, c cid.Cid, kind SettleKind, at time.Time)
}
