package sphinx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type observedAttempt struct {
	jobID   uint64
	c       cid.Cid
	attempt int
	paths   []BranchPath
}

type observedOutcome struct {
	jobID   uint64
	attempt int
	branch  int
	outcome BranchOutcome
	at      time.Time
}

type observedSettle struct {
	jobID uint64
	c     cid.Cid
	kind  SettleKind
	at    time.Time
}

// recordingObserver captures every event; snapshots copy under the lock
type recordingObserver struct {
	mu       sync.Mutex
	attempts []observedAttempt
	outcomes []observedOutcome
	settles  []observedSettle
}

func (o *recordingObserver) OnAttempt(jobID uint64, c cid.Cid, attempt int, paths []BranchPath) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, observedAttempt{jobID, c, attempt, paths})
}

func (o *recordingObserver) OnBranchOutcome(jobID uint64, attempt, branch int, outcome BranchOutcome, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.outcomes = append(o.outcomes, observedOutcome{jobID, attempt, branch, outcome, at})
}

func (o *recordingObserver) OnSettled(jobID uint64, c cid.Cid, kind SettleKind, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.settles = append(o.settles, observedSettle{jobID, c, kind, at})
}

func (o *recordingObserver) snapshotAttempts() []observedAttempt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedAttempt(nil), o.attempts...)
}

func (o *recordingObserver) snapshotOutcomes() []observedOutcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedOutcome(nil), o.outcomes...)
}

func (o *recordingObserver) snapshotSettles() []observedSettle {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedSettle(nil), o.settles...)
}

// the observer sees the final slot assignment of the attempt: k branch
// paths, forward with the proxy as last hop, m return pairs, all peers
// pairwise distinct
func TestObserverSeesAttemptPaths(t *testing.T) {
	obs := &recordingObserver{}
	k, m := 2, ReturnPathsPerJob
	sender := &fakeSender{}
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, Observer: obs})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	atts := obs.snapshotAttempts()
	if len(atts) != 1 {
		t.Fatalf("observer saw %d attempts, want 1", len(atts))
	}
	a := atts[0]
	if a.attempt != 1 {
		t.Errorf("attempt number = %d, want 1", a.attempt)
	}
	if len(a.paths) != k {
		t.Fatalf("attempt carries %d branch paths, want k = %d", len(a.paths), k)
	}
	seen := make(map[peer.ID]bool)
	for bi, p := range a.paths {
		if len(p.Forward) != NrHops {
			t.Errorf("branch %d forward path has %d hops, want %d", bi, len(p.Forward), NrHops)
		}
		if len(p.Returns) != m {
			t.Errorf("branch %d has %d return paths, want m = %d", bi, len(p.Returns), m)
		}
		all := append([]peer.ID(nil), p.Forward...)
		for ri, ret := range p.Returns {
			if len(ret) != NrHops-1 {
				t.Errorf("branch %d return %d has %d relays, want %d", bi, ri, len(ret), NrHops-1)
			}
			all = append(all, ret...)
		}
		for _, pid := range all {
			if seen[pid] {
				t.Errorf("peer %s appears twice in the attempt", pid)
			}
			seen[pid] = true
		}
	}
	if len(seen) != jobSampleSize(k, m) {
		t.Errorf("attempt uses %d distinct peers, want %d", len(seen), jobSampleSize(k, m))
	}

	// pins export-vs-wire identity: every exported forward path's first
	// slot must name a peer a packet actually went to, one per branch. The
	// branches are dispatched together, so snapshot order says nothing
	// about branch order
	sent := sender.snapshot()
	if len(sent) != k {
		t.Fatalf("sender recorded %d packets, want k = %d", len(sent), k)
	}
	wire := make(map[peer.ID]bool, k)
	for _, s := range sent {
		wire[s.to] = true
	}
	for bi := range a.paths {
		first := a.paths[bi].Forward[0]
		if !wire[first] {
			t.Errorf("branch %d: exported first hop %s got no packet", bi, first)
		}
		delete(wire, first)
	}
}

func okReply(t *testing.T, n int) []byte {
	t.Helper()
	payload, err := EncodeReply(ReplyStatusOK, testProviders(t, n))
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	return payload
}

func TestObserverBranchOK(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m), JobConfig{Branches: 1, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	before := time.Now()
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], okReply(t, 2))
	<-res

	outs := obs.snapshotOutcomes()
	if len(outs) != 1 {
		t.Fatalf("observer saw %d outcomes, want 1", len(outs))
	}
	o := outs[0]
	if o.outcome != BranchOK || o.attempt != 1 || o.branch != 0 {
		t.Errorf("outcome = %s attempt %d branch %d, want ok/1/0", o.outcome, o.attempt, o.branch)
	}
	if o.at.Before(before) || o.at.After(time.Now()) {
		t.Errorf("outcome time %v outside the test window", o.at)
	}
}

func TestObserverBranchFailed(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m),
		JobConfig{Branches: 1, DisableRetransmit: true, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	payload, err := EncodeReply(ReplyStatusFailed, nil)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], payload)
	<-res

	outs := obs.snapshotOutcomes()
	if len(outs) != 1 || outs[0].outcome != BranchFailed {
		t.Fatalf("outcomes = %v, want a single failed", outs)
	}
}

func TestObserverBranchExhausted(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m),
		JobConfig{Branches: 1, DisableRetransmit: true, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	// authenticated garbage through every envelope exhausts the branch
	for _, id := range jm.pendingSURBIDs() {
		jm.HandleSURBReply(id, []byte{0xFF})
	}
	<-res

	outs := obs.snapshotOutcomes()
	if len(outs) != 1 || outs[0].outcome != BranchExhausted {
		t.Fatalf("outcomes = %v, want a single exhausted", outs)
	}
}

func TestObserverBranchDeadSend(t *testing.T) {
	obs := &recordingObserver{}
	k, m := 2, ReturnPathsPerJob
	sender := &flakySender{failFirst: 1}
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	jm.HandleSURBReply(jm.pendingSURBIDs()[0], okReply(t, 1))
	<-res

	var dead []observedOutcome
	for _, o := range obs.snapshotOutcomes() {
		if o.outcome == BranchDeadSend {
			dead = append(dead, o)
		}
	}
	if len(dead) != 1 || dead[0].attempt != 1 {
		t.Fatalf("dead-send outcomes = %v, want exactly one on attempt 1", dead)
	}
	// the branches are dispatched together, so which one the sender fails
	// is not fixed; the wire says which branch died
	sent := sender.snapshot()
	if len(sent) != 1 {
		t.Fatalf("sender recorded %d packets, want the single live branch", len(sent))
	}
	paths := obs.snapshotAttempts()[0].paths
	if first := paths[dead[0].branch].Forward[0]; first == sent[0].to {
		t.Errorf("dead send names branch %d, whose packet reached %s", dead[0].branch, first)
	}
}

// OnSettled fires before the result delivery, so a settle event is
// always visible once the channel yields
func TestObserverSettleSuccessBeforeDelivery(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m), JobConfig{Branches: 1, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	jm.HandleSURBReply(jm.pendingSURBIDs()[0], okReply(t, 1))
	<-res

	settles := obs.snapshotSettles()
	if len(settles) != 1 || settles[0].kind != SettleSuccess {
		t.Fatalf("settles = %v, want a single success recorded before delivery", settles)
	}
	if settles[0].at.IsZero() {
		t.Error("settle time is zero")
	}
}

func TestObserverSettleTimeout(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m),
		JobConfig{Branches: 1, DisableRetransmit: true, Timeout: 50 * time.Millisecond, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	got := <-res
	if !errors.Is(got.Err, ErrJobTimeout) {
		t.Fatalf("job settled with %v, want ErrJobTimeout", got.Err)
	}
	settles := obs.snapshotSettles()
	if len(settles) != 1 || settles[0].kind != SettleTimeout {
		t.Fatalf("settles = %v, want a single timeout", settles)
	}
}

func TestObserverSettleClosed(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m), JobConfig{Branches: 1, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	jm.Close()
	<-res
	settles := obs.snapshotSettles()
	if len(settles) != 1 || settles[0].kind != SettleClosed {
		t.Fatalf("settles = %v, want a single closed", settles)
	}
}

// a failed first attempt fires a second OnAttempt with fresh paths
func TestObserverRetransmitSecondAttempt(t *testing.T) {
	obs := &recordingObserver{}
	m := ReturnPathsPerJob
	// twice the demand so the retry can exclude the first attempt's relays
	jm, _ := newTestJobManager(t, &fakeSender{}, 2*jobSampleSize(1, m), JobConfig{Branches: 1, Observer: obs})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	firstIDs := jm.pendingSURBIDs()
	payload, err := EncodeReply(ReplyStatusFailed, nil)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(firstIDs[0], payload)

	// the retransmit registers fresh SURBs off the delivery goroutine
	waitUntil(t, 2*time.Second, func() bool {
		return len(obs.snapshotAttempts()) == 2
	}, "second attempt never registered")

	jm.HandleSURBReply(jm.pendingSURBIDs()[0], okReply(t, 1))
	got := <-res
	if got.Err != nil {
		t.Fatalf("job settled with %v, want success on attempt 2", got.Err)
	}

	atts := obs.snapshotAttempts()
	if atts[0].attempt != 1 || atts[1].attempt != 2 {
		t.Fatalf("attempt numbers = %d,%d want 1,2", atts[0].attempt, atts[1].attempt)
	}
	if atts[1].paths[0].Forward[0] == atts[0].paths[0].Forward[0] {
		t.Error("retry reused the first attempt's first hop despite a big enough pool")
	}
	settles := obs.snapshotSettles()
	if len(settles) != 1 || settles[0].kind != SettleSuccess {
		t.Fatalf("settles = %v, want a single success", settles)
	}
}

// an observer may call back into the manager: emissions run outside the lock
type reentrantObserver struct {
	recordingObserver
	jm *JobManager
}

func (o *reentrantObserver) OnSettled(jobID uint64, c cid.Cid, kind SettleKind, at time.Time) {
	o.jm.pendingCounts() // deadlocks if the emit held jm.mu
	o.recordingObserver.OnSettled(jobID, c, kind, at)
}

func TestObserverReentrantSafe(t *testing.T) {
	obs := &reentrantObserver{}
	m := ReturnPathsPerJob
	jm, _ := newTestJobManager(t, &fakeSender{}, jobSampleSize(1, m), JobConfig{Branches: 1, Observer: obs})
	obs.jm = jm
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], okReply(t, 1))
	<-res
	if len(obs.snapshotSettles()) != 1 {
		t.Fatal("reentrant observer missed the settle event")
	}
}
