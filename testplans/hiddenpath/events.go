package main

import (
	"sort"
	"sync"
	"time"

	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
	"github.com/ipfs/go-cid"
)

// probeEvent is one completed Probe call; FirstHaveMs counts from the
// probe start, -1 without a hit
type probeEvent struct {
	Cid         string  `json:"cid"`
	Hit         bool    `json:"hit"`
	FirstHaveMs float64 `json:"first_have_ms"`
	Responders  int     `json:"responders"`
}

// branchExport is one branch of one attempt; peers as string IDs so the
// analysis joins them against victim_peers
type branchExport struct {
	Forward    []string   `json:"forward"`
	Returns    [][]string `json:"returns"`
	Outcome    string     `json:"outcome"`
	AnsweredMs float64    `json:"answered_ms"`
}

type attemptExport struct {
	Attempt  int            `json:"attempt"`
	Branches []branchExport `json:"branches"`
}

type outcomeRec struct {
	attempt, branch int
	outcome         sphinx.BranchOutcome
	at              time.Time
}

type jobEvents struct {
	paths    map[int][]sphinx.BranchPath
	outcomes []outcomeRec
	settleAt time.Time
	settled  string
}

// eventLog collects sphinx job and probe events across a run; all methods
// are safe for concurrent use
type eventLog struct {
	mu          sync.Mutex
	jobs        map[string]*jobEvents
	jobCID      map[uint64]string
	probes      []probeEvent
	walks       []walkEvent
	firstHave   map[string]time.Time
	firstRecord map[string]time.Time
}

func newEventLog() *eventLog {
	return &eventLog{
		jobs:        make(map[string]*jobEvents),
		jobCID:      make(map[uint64]string),
		firstHave:   make(map[string]time.Time),
		firstRecord: make(map[string]time.Time),
	}
}

func (l *eventLog) OnAttempt(jobID uint64, c cid.Cid, attempt int, paths []sphinx.BranchPath) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := c.String()
	l.jobCID[jobID] = key
	je := l.jobs[key]
	if je == nil {
		je = &jobEvents{paths: make(map[int][]sphinx.BranchPath)}
		l.jobs[key] = je
	}
	je.paths[attempt] = paths
}

func (l *eventLog) OnBranchOutcome(jobID uint64, attempt, branch int, outcome sphinx.BranchOutcome, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	je := l.jobs[l.jobCID[jobID]]
	if je == nil {
		return
	}
	je.outcomes = append(je.outcomes, outcomeRec{attempt, branch, outcome, at})
}

func (l *eventLog) OnSettled(jobID uint64, c cid.Cid, kind sphinx.SettleKind, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	je := l.jobs[c.String()]
	if je == nil {
		return
	}
	je.settled, je.settleAt = kind.String(), at
}

func (l *eventLog) onProbe(c cid.Cid, hit bool, start, firstHave time.Time, responders int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ms := -1.0
	if hit {
		ms = msBetween(start, firstHave)
		key := c.String()
		if _, ok := l.firstHave[key]; !ok {
			l.firstHave[key] = firstHave
		}
	}
	l.probes = append(l.probes, probeEvent{Cid: c.String(), Hit: hit, FirstHaveMs: ms, Responders: responders})
}

// msBetween is the export clock: -1 stands for "never happened"
func msBetween(a, b time.Time) float64 {
	if a.IsZero() || b.IsZero() {
		return -1
	}
	return float64(b.Sub(a).Microseconds()) / 1000.0
}

// exportJob assembles one job's attempt structure; branches without an
// outcome stay silent
func (l *eventLog) exportJob(c cid.Cid, jobStart time.Time) (attempts []attemptExport, firstOKMs, settledMs float64, settleKind string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	firstOKMs, settledMs = -1, -1
	je := l.jobs[c.String()]
	if je == nil {
		return nil, firstOKMs, settledMs, ""
	}

	nums := make([]int, 0, len(je.paths))
	for a := range je.paths {
		nums = append(nums, a)
	}
	sort.Ints(nums)
	for _, a := range nums {
		paths := je.paths[a]
		branches := make([]branchExport, len(paths))
		for bi, p := range paths {
			fwd := make([]string, len(p.Forward))
			for i, pid := range p.Forward {
				fwd[i] = pid.String()
			}
			rets := make([][]string, len(p.Returns))
			for ri, ret := range p.Returns {
				rets[ri] = make([]string, len(ret))
				for i, pid := range ret {
					rets[ri][i] = pid.String()
				}
			}
			branches[bi] = branchExport{Forward: fwd, Returns: rets, Outcome: "silent", AnsweredMs: -1}
		}
		attempts = append(attempts, attemptExport{Attempt: a, Branches: branches})
	}
	for _, o := range je.outcomes {
		for ai := range attempts {
			if attempts[ai].Attempt != o.attempt || o.branch >= len(attempts[ai].Branches) {
				continue
			}
			b := &attempts[ai].Branches[o.branch]
			b.Outcome = o.outcome.String()
			b.AnsweredMs = msBetween(jobStart, o.at)
			if o.outcome == sphinx.BranchOK {
				if ms := msBetween(jobStart, o.at); firstOKMs < 0 || ms < firstOKMs {
					firstOKMs = ms
				}
			}
		}
	}
	settledMs = msBetween(jobStart, je.settleAt)
	return attempts, firstOKMs, settledMs, je.settled
}

// walkEvent is one completed route-stage DHT walk; FirstRecordMs counts
// from the walk start, -1 without a record
type walkEvent struct {
	Cid           string  `json:"cid"`
	Found         bool    `json:"found"`
	FirstRecordMs float64 `json:"first_record_ms"`
	TotalMs       float64 `json:"total_ms"`
	Providers     int     `json:"providers"`
}

func (l *eventLog) onWalk(c cid.Cid, start, firstRecord, end time.Time, providers int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := c.String()
	if !firstRecord.IsZero() {
		if _, ok := l.firstRecord[key]; !ok {
			l.firstRecord[key] = firstRecord
		}
	}
	l.walks = append(l.walks, walkEvent{Cid: key, Found: !firstRecord.IsZero(),
		FirstRecordMs: msBetween(start, firstRecord), TotalMs: msBetween(start, end), Providers: providers})
}

func (l *eventLog) dhtFirstRecordMs(c cid.Cid, jobStart time.Time) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	at, ok := l.firstRecord[c.String()]
	// a stamp before the job start is a stale entry for a reused CID
	if !ok || at.Before(jobStart) {
		return -1
	}
	return msBetween(jobStart, at)
}

func (l *eventLog) walkSnapshot() []walkEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]walkEvent(nil), l.walks...)
}

func (l *eventLog) probeFirstHaveMs(c cid.Cid, jobStart time.Time) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	at, ok := l.firstHave[c.String()]
	if !ok {
		return -1
	}
	return msBetween(jobStart, at)
}

func (l *eventLog) probeSnapshot() []probeEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]probeEvent(nil), l.probes...)
}
