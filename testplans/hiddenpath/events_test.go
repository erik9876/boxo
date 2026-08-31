package main

import (
	"testing"
	"time"

	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func pathWith(prefix string) sphinx.BranchPath {
	return sphinx.BranchPath{
		Forward: []peer.ID{peer.ID(prefix + "-r1"), peer.ID(prefix + "-r2"), peer.ID(prefix + "-px")},
		Returns: [][]peer.ID{
			{peer.ID(prefix + "-q11"), peer.ID(prefix + "-q12")},
			{peer.ID(prefix + "-q21"), peer.ID(prefix + "-q22")},
		},
	}
}

func mustTestCID(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("events-test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func TestEventLogExportsJob(t *testing.T) {
	l := newEventLog()
	c := mustTestCID(t)
	start := time.Now()

	l.OnAttempt(1, c, 1, []sphinx.BranchPath{pathWith("a"), pathWith("b")})
	l.OnBranchOutcome(1, 1, 0, sphinx.BranchOK, start.Add(100*time.Millisecond))
	// branch 1 stays silent
	l.OnSettled(1, c, sphinx.SettleSuccess, start.Add(150*time.Millisecond))

	attempts, firstOK, settled, kind := l.exportJob(c, start)
	if len(attempts) != 1 || attempts[0].Attempt != 1 {
		t.Fatalf("attempts = %+v, want one attempt 1", attempts)
	}
	br := attempts[0].Branches
	if len(br) != 2 {
		t.Fatalf("branches = %d, want 2", len(br))
	}
	if br[0].Outcome != "ok" || br[0].AnsweredMs < 99 || br[0].AnsweredMs > 101 {
		t.Errorf("branch 0 = %+v, want ok at ~100 ms", br[0])
	}
	if br[1].Outcome != "silent" || br[1].AnsweredMs != -1 {
		t.Errorf("branch 1 = %+v, want silent/-1", br[1])
	}
	if len(br[0].Forward) != 3 || len(br[0].Returns) != 2 {
		t.Errorf("branch 0 path shape = %d fwd / %d ret, want 3/2", len(br[0].Forward), len(br[0].Returns))
	}
	if firstOK < 99 || firstOK > 101 {
		t.Errorf("firstOK = %.1f, want ~100", firstOK)
	}
	if settled < 149 || settled > 151 {
		t.Errorf("settled = %.1f, want ~150", settled)
	}
	if kind != "success" {
		t.Errorf("kind = %q, want success", kind)
	}
}

func TestEventLogExportsRetryAttempt(t *testing.T) {
	l := newEventLog()
	c := mustTestCID(t)
	start := time.Now()

	l.OnAttempt(1, c, 1, []sphinx.BranchPath{pathWith("a")})
	l.OnBranchOutcome(1, 1, 0, sphinx.BranchFailed, start.Add(50*time.Millisecond))
	l.OnAttempt(1, c, 2, []sphinx.BranchPath{pathWith("c")})
	l.OnBranchOutcome(1, 2, 0, sphinx.BranchOK, start.Add(200*time.Millisecond))
	l.OnSettled(1, c, sphinx.SettleSuccess, start.Add(210*time.Millisecond))

	attempts, firstOK, _, _ := l.exportJob(c, start)
	if len(attempts) != 2 || attempts[0].Attempt != 1 || attempts[1].Attempt != 2 {
		t.Fatalf("attempts = %+v, want 1 then 2", attempts)
	}
	if attempts[0].Branches[0].Outcome != "failed" || attempts[1].Branches[0].Outcome != "ok" {
		t.Errorf("outcomes = %s/%s, want failed/ok", attempts[0].Branches[0].Outcome, attempts[1].Branches[0].Outcome)
	}
	if firstOK < 199 || firstOK > 201 {
		t.Errorf("firstOK = %.1f, want ~200 (the retry's answer)", firstOK)
	}
}

func TestEventLogProbeFirstHave(t *testing.T) {
	l := newEventLog()
	c := mustTestCID(t)
	jobStart := time.Now()
	probeStart := jobStart.Add(5 * time.Millisecond)

	l.onProbe(c, true, probeStart, probeStart.Add(200*time.Millisecond), 1)
	l.onProbe(c, false, probeStart, time.Time{}, 0)

	if ms := l.probeFirstHaveMs(c, jobStart); ms < 204 || ms > 206 {
		t.Errorf("probeFirstHaveMs = %.1f, want ~205", ms)
	}
	events := l.probeSnapshot()
	if len(events) != 2 || !events[0].Hit || events[1].Hit {
		t.Fatalf("probe events = %+v, want hit then miss", events)
	}
	if events[0].FirstHaveMs < 199 || events[0].FirstHaveMs > 201 {
		t.Errorf("event first-have = %.1f, want ~200 (relative to probe start)", events[0].FirstHaveMs)
	}
	if events[1].FirstHaveMs != -1 {
		t.Errorf("miss first-have = %.1f, want -1", events[1].FirstHaveMs)
	}
}

func TestEventLogWalkFirstRecord(t *testing.T) {
	l := newEventLog()
	c := mustTestCID(t)
	jobStart := time.Now()
	walkStart := jobStart.Add(5 * time.Millisecond)

	l.onWalk(c, walkStart, walkStart.Add(200*time.Millisecond), walkStart.Add(900*time.Millisecond), 1)
	// a later walk for the same CID must not move the job-level instant
	l.onWalk(c, walkStart, walkStart.Add(400*time.Millisecond), walkStart.Add(950*time.Millisecond), 2)
	l.onWalk(c, walkStart, time.Time{}, walkStart.Add(300*time.Millisecond), 0)

	if ms := l.dhtFirstRecordMs(c, jobStart); ms < 204 || ms > 206 {
		t.Errorf("dhtFirstRecordMs = %.1f, want ~205", ms)
	}
	if ms := l.dhtFirstRecordMs(mustOtherCID(t), jobStart); ms != -1 {
		t.Errorf("unknown cid dhtFirstRecordMs = %.1f, want -1", ms)
	}

	walks := l.walkSnapshot()
	if len(walks) != 3 {
		t.Fatalf("walk events = %d, want 3", len(walks))
	}
	if !walks[0].Found || walks[0].FirstRecordMs < 199 || walks[0].FirstRecordMs > 201 {
		t.Errorf("walk 0 = %+v, want found at ~200 ms after walk start", walks[0])
	}
	if walks[0].TotalMs < 899 || walks[0].TotalMs > 901 {
		t.Errorf("walk 0 total = %.1f, want ~900", walks[0].TotalMs)
	}
	if walks[2].Found || walks[2].FirstRecordMs != -1 {
		t.Errorf("empty walk = %+v, want not found / -1", walks[2])
	}
}

func TestEventLogWalkStaleRecordIsSentinel(t *testing.T) {
	l := newEventLog()
	c := mustTestCID(t)
	jobStart := time.Now()

	// a walk for a reused CID that finished before this job started must
	// not export a negative pseudo-latency
	l.onWalk(c, jobStart.Add(-2*time.Second), jobStart.Add(-time.Second), jobStart.Add(-500*time.Millisecond), 1)
	if ms := l.dhtFirstRecordMs(c, jobStart); ms != -1 {
		t.Errorf("stale record exported %.1f, want -1", ms)
	}
}

func mustOtherCID(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("events-test-other"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}
