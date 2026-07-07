package sphinx

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
)

// pendingCounts reports registered jobs and SURB mappings; test hook
func (jm *JobManager) pendingCounts() (jobs, surbs int) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	return len(jm.jobs), len(jm.bySURB)
}

// pendingSURBIDs snapshots the outstanding SURB IDs; test hook
func (jm *JobManager) pendingSURBIDs() []SURBID {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	ids := make([]SURBID, 0, len(jm.bySURB))
	for id := range jm.bySURB {
		ids = append(ids, id)
	}
	return ids
}

// pendingJobRelays snapshots the drawn-relay ledger of the single pending
// job; test hook. The ledger is a set, so its size equals the number of
// pairwise-distinct relays drawn across all branches and attempts
func (jm *JobManager) pendingJobRelays(t *testing.T) map[peer.ID]struct{} {
	t.Helper()
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if len(jm.jobs) != 1 {
		t.Fatalf("want exactly 1 pending job, have %d", len(jm.jobs))
	}
	for _, j := range jm.jobs {
		out := make(map[peer.ID]struct{}, len(j.relays))
		for pid := range j.relays {
			out[pid] = struct{}{}
		}
		return out
	}
	return nil
}

// fillPool puts n freshly generated relay records into pool
func fillPool(t *testing.T, pool *KeyStore, n int) {
	t.Helper()
	for range n {
		idPriv, pid := newIdentity(t)
		km, err := NewKeyManager(idPriv, time.Hour)
		if err != nil {
			t.Fatalf("NewKeyManager: %v", err)
		}
		if err := pool.Put(NewKeyRecord(pid, km.PublicKey(), time.Hour)); err != nil {
			t.Fatalf("pool.Put: %v", err)
		}
	}
}

// newTestJobManager wires a JobManager against sender with a pool of
// poolSize live relays
func newTestJobManager(t *testing.T, sender PacketSender, poolSize int, cfg JobConfig) (*JobManager, *SURBStore) {
	t.Helper()
	idPriv, _ := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	pool := NewKeyStore()
	fillPool(t, pool, poolSize)
	surbs := NewSURBStore()
	jm, err := NewJobManager(sender, km, pool, surbs, cfg)
	if err != nil {
		t.Fatalf("NewJobManager: %v", err)
	}
	return jm, surbs
}

// jobSampleSize is the pool demand of one attempt: k branch groups of the
// forward path (2 relays + proxy) plus 2 relays per return path
func jobSampleSize(k, m int) int { return k * (NrHops + 2*m) }

// expirePool advances pool's clock past every record's TTL, so the next
// draw comes back empty. The write shares the store's mutex with the
// sampling reads, so racing a live retransmit is safe
func expirePool(pool *KeyStore) {
	pool.mu.Lock()
	pool.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	pool.mu.Unlock()
}

// flakySender fails the first failFirst sends and captures the rest
type flakySender struct {
	fakeSender
	flakeMu   sync.Mutex
	failFirst int
	calls     int
}

func (s *flakySender) SendPacket(ctx context.Context, next peer.ID, pkt []byte) error {
	s.flakeMu.Lock()
	s.calls++
	fail := s.calls <= s.failFirst
	s.flakeMu.Unlock()
	if fail {
		return errors.New("branch send failed by test")
	}
	return s.fakeSender.SendPacket(ctx, next, pkt)
}

func TestStartJobSendsForwardPacket(t *testing.T) {
	sender := &fakeSender{}
	m := ReturnPathsPerJob
	// Branches pinned to 1: this test covers the per-branch mechanics that
	// k multiplies; TestStartJobSendsKBranchPackets covers the default k
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(1, m), JobConfig{Branches: 1})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if res == nil {
		t.Fatal("StartJob returned no result channel")
	}

	sent := sender.snapshot()
	if len(sent) != 1 {
		t.Fatalf("StartJob sent %d packets, want 1", len(sent))
	}
	if len(sent[0].pkt) != Geometry().PacketLength {
		t.Errorf("forward packet is %d bytes, want %d", len(sent[0].pkt), Geometry().PacketLength)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 1 || surbIDs != m {
		t.Errorf("pending state = %d jobs / %d surb ids, want 1 / %d", jobs, surbIDs, m)
	}
	if surbs.Len() != m {
		t.Errorf("surb store holds %d entries, want %d", surbs.Len(), m)
	}
}

func TestStartJobSendsKBranchPackets(t *testing.T) {
	sender := &fakeSender{}
	k, m := DefaultBranchesPerJob, ReturnPathsPerJob
	// The zero config exercises the default k = 2
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	sent := sender.snapshot()
	if len(sent) != k {
		t.Fatalf("StartJob sent %d packets, want k = %d", len(sent), k)
	}
	firstHops := make(map[peer.ID]bool, k)
	for i, s := range sent {
		if len(s.pkt) != Geometry().PacketLength {
			t.Errorf("branch packet %d is %d bytes, want %d", i, len(s.pkt), Geometry().PacketLength)
		}
		if firstHops[s.to] {
			t.Errorf("branches share first hop %s", s.to)
		}
		firstHops[s.to] = true
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 1 || surbIDs != k*m {
		t.Errorf("pending state = %d jobs / %d surb ids, want 1 / %d", jobs, surbIDs, k*m)
	}
	if surbs.Len() != k*m {
		t.Errorf("surb store holds %d entries, want k·m = %d", surbs.Len(), k*m)
	}
}

func TestJobSampleFullyDisjoint(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, ReturnPathsPerJob
	need := jobSampleSize(k, m)
	// With the pool exactly one attempt's demand, a full draw must use
	// every live entry exactly once: the job's relay ledger holding need
	// distinct peers proves all branch groups pairwise disjoint
	jm, _ := newTestJobManager(t, sender, need, JobConfig{Branches: k})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if relays := jm.pendingJobRelays(t); len(relays) != need {
		t.Errorf("job drew %d distinct relays, want %d (fully disjoint)", len(relays), need)
	}
}

func TestBranchSendFailureDoesNotFailJob(t *testing.T) {
	sender := &flakySender{failFirst: 1}
	k, m := 2, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k})
	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob with one dead branch: %v", err)
	}
	met := jm.Metrics()
	if met.SendFailures != 1 || met.BranchPacketsSent != 1 {
		t.Errorf("send counters = %d failed / %d sent, want 1 / 1", met.SendFailures, met.BranchPacketsSent)
	}

	// A reply through any registered SURB settles the job as usual
	want := testProviders(t, 3)
	payload, err := EncodeReply(ReplyStatusOK, want)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], payload)
	got, ok := <-res
	if !ok || got.Err != nil {
		t.Fatalf("job settled with ok=%v err=%v, want success", ok, got.Err)
	}
	assertProvidersEqual(t, got.Providers, want)
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after success", surbs.Len())
	}
}

func TestFirstReplyWinsAndDuplicatesAreIgnored(t *testing.T) {
	sender := &fakeSender{}
	m := ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(1, m), JobConfig{Branches: 1})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	ids := jm.pendingSURBIDs()
	if len(ids) != m {
		t.Fatalf("job registered %d surb ids, want %d", len(ids), m)
	}

	want := testProviders(t, 3)
	payload, err := EncodeReply(ReplyStatusOK, want)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}

	// First reply settles the job
	jm.HandleSURBReply(ids[0], payload)
	got, ok := <-res
	if !ok {
		t.Fatal("result channel closed without a result")
	}
	if got.Err != nil {
		t.Fatalf("job settled with error: %v", got.Err)
	}
	assertProvidersEqual(t, got.Providers, want)

	// All state is gone, including the m−1 unused SURB entries
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after reply: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after reply", surbs.Len())
	}

	// Duplicates through the remaining SURBs are ignored; the channel is
	// closed and yields no second result
	jm.HandleSURBReply(ids[1], payload)
	if _, ok := <-res; ok {
		t.Error("a duplicate reply produced a second result")
	}
}

func TestDuplicateRepliesAcrossBranchesIgnored(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, 1 // one SURB per branch: two IDs, guaranteed on different branches
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	ids := jm.pendingSURBIDs()
	if len(ids) != k*m {
		t.Fatalf("job registered %d surb ids, want %d", len(ids), k*m)
	}

	want := testProviders(t, 2)
	payload, err := EncodeReply(ReplyStatusOK, want)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(ids[0], payload)
	got, ok := <-res
	if !ok || got.Err != nil {
		t.Fatalf("job settled with ok=%v err=%v, want success", ok, got.Err)
	}

	// The other branch's proxy answers too; the duplicate dies at the job table
	jm.HandleSURBReply(ids[1], payload)
	if _, ok := <-res; ok {
		t.Error("a cross-branch duplicate produced a second result")
	}
	met := jm.Metrics()
	if met.RepliesWon != 1 || met.RepliesDuplicate != 1 {
		t.Errorf("reply counters = %d won / %d duplicate, want 1 / 1", met.RepliesWon, met.RepliesDuplicate)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after duplicates", surbs.Len())
	}
}

func TestFinalTimeoutScrubsAllSURBEntries(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, Timeout: 100 * time.Millisecond})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	// Both attempt timers must run out: the retransmit wave, then the
	// final timeout
	select {
	case got := <-res:
		if !errors.Is(got.Err, ErrJobTimeout) {
			t.Fatalf("timed-out job settled with %v, want ErrJobTimeout", got.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never timed out")
	}

	if sent := len(sender.snapshot()); sent != 2*k {
		t.Errorf("job sent %d packets over both attempts, want 2·k = %d", sent, 2*k)
	}
	met := jm.Metrics()
	if met.Retransmits != 1 || met.JobsTimedOut != 1 {
		t.Errorf("counters = %d retransmits / %d timed out, want 1 / 1", met.Retransmits, met.JobsTimedOut)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after timeout: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after final timeout; both attempts' entries must be deleted", surbs.Len())
	}
}

// TestDisableRetransmitSingleAttempt: with DisableRetransmit the first
// attempt's timer is the final timer: the job settles as ErrJobTimeout
// after exactly one wave of k packets, no retransmit is ever launched, and
// the scrub still clears all state
func TestDisableRetransmitSingleAttempt(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, ReturnPathsPerJob
	// Pool sized for two attempts: proves the missing second wave is the
	// flag's doing, not a pool shortfall
	jm, surbs := newTestJobManager(t, sender, 2*jobSampleSize(k, m),
		JobConfig{Branches: k, Timeout: 100 * time.Millisecond, DisableRetransmit: true})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	select {
	case got := <-res:
		if !errors.Is(got.Err, ErrJobTimeout) {
			t.Fatalf("timed-out job settled with %v, want ErrJobTimeout", got.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never timed out")
	}

	// No second wave, not even after another timeout period
	time.Sleep(300 * time.Millisecond)
	if sent := len(sender.snapshot()); sent != k {
		t.Errorf("job sent %d packets, want exactly k = %d (single attempt)", sent, k)
	}
	met := jm.Metrics()
	if met.Retransmits != 0 || met.RetransmitsFailed != 0 {
		t.Errorf("retransmit counters = %d / %d failed, want 0 / 0", met.Retransmits, met.RetransmitsFailed)
	}
	if met.JobsTimedOut != 1 {
		t.Errorf("JobsTimedOut = %d, want 1", met.JobsTimedOut)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after timeout: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after the single-attempt timeout", surbs.Len())
	}
}

func TestRetransmitLateFirstAttemptReplyWins(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, 2
	need := jobSampleSize(k, m)
	jm, surbs := newTestJobManager(t, sender, 2*need, JobConfig{Branches: k, ReturnPaths: m, Timeout: 200 * time.Millisecond})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	// Snapshot the first attempt's SURB IDs before the retransmit can add
	// more
	ids1 := jm.pendingSURBIDs()
	if len(ids1) != k*m {
		t.Fatalf("first attempt registered %d surb ids, want %d", len(ids1), k*m)
	}

	waitFor(t, 5*time.Second, "the retransmit wave", func() bool { return len(sender.snapshot()) == 2*k })
	met := jm.Metrics()
	if met.Retransmits != 1 || met.RetransmitsFailed != 0 {
		t.Fatalf("retransmit counters = %d / %d failed, want 1 / 0", met.Retransmits, met.RetransmitsFailed)
	}
	if surbs.Len() != 2*k*m {
		t.Errorf("surb store holds %d entries after the retransmit, want 2·k·m = %d", surbs.Len(), 2*k*m)
	}

	// A late reply through a FIRST-attempt SURB still wins: those entries
	// stay registered until the job ends
	want := testProviders(t, 3)
	payload, err := EncodeReply(ReplyStatusOK, want)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(ids1[0], payload)
	got, ok := <-res
	if !ok || got.Err != nil {
		t.Fatalf("job settled with ok=%v err=%v, want success from the late reply", ok, got.Err)
	}
	assertProvidersEqual(t, got.Providers, want)

	// Exactly one retry: no third wave after another timeout period
	time.Sleep(300 * time.Millisecond)
	if sent := len(sender.snapshot()); sent != 2*k {
		t.Errorf("job sent %d packets, want exactly 2·k = %d (one retransmit)", sent, 2*k)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after settle: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after settle; all 2·k·m must be deleted", surbs.Len())
	}
}

func TestRetransmitPrefersFreshRelays(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	need := jobSampleSize(k, m)
	// Pool of 2·need: the retransmit draw must avoid every first-attempt
	// relay entirely
	jm, _ := newTestJobManager(t, sender, 2*need, JobConfig{Branches: k, ReturnPaths: m, Timeout: 300 * time.Millisecond})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	waitFor(t, 5*time.Second, "the retransmit wave", func() bool { return jm.Metrics().Retransmits == 1 })

	// The relay ledger spans both attempts; 2·need distinct entries prove
	// the second draw shared no relay with the first
	if relays := jm.pendingJobRelays(t); len(relays) != 2*need {
		t.Errorf("job drew %d distinct relays over two attempts, want %d (fully disjoint)", len(relays), 2*need)
	}
}

func TestRetransmitExclusionFallsBack(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	need := jobSampleSize(k, m)
	// Pool between need and 2·need: the exclusion would starve the draw,
	// so the retransmit falls back to a plain fresh sample; overlap with
	// the first attempt allowed, never a degraded job
	jm, surbs := newTestJobManager(t, sender, need+2, JobConfig{Branches: k, ReturnPaths: m, Timeout: 150 * time.Millisecond})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	waitFor(t, 5*time.Second, "the retransmit wave", func() bool { return len(sender.snapshot()) == 2*k })
	met := jm.Metrics()
	if met.Retransmits != 1 || met.RetransmitsFailed != 0 {
		t.Errorf("retransmit counters = %d / %d failed, want 1 / 0", met.Retransmits, met.RetransmitsFailed)
	}

	// Settle cleanly through a still-registered SURB
	payload, err := EncodeReply(ReplyStatusOK, nil)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], payload)
	if got, ok := <-res; !ok || got.Err != nil {
		t.Fatalf("job settled with ok=%v err=%v, want success", ok, got.Err)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after settle", surbs.Len())
	}
}

func TestRetransmitPoolTooSmallDegrades(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, Timeout: 150 * time.Millisecond})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	// Kill the pool before the retransmit draws: the retransmit must
	// degrade, not fail the job: the first attempt's SURBs stay live
	// until the final timer
	expirePool(jm.pool)

	waitFor(t, 5*time.Second, "the degraded retransmit", func() bool { return jm.Metrics().RetransmitsFailed == 1 })
	if surbs.Len() != k*m {
		t.Errorf("surb store holds %d entries after the degrade, want the first attempt's %d", surbs.Len(), k*m)
	}

	select {
	case got := <-res:
		if !errors.Is(got.Err, ErrJobTimeout) {
			t.Fatalf("degraded job settled with %v, want ErrJobTimeout", got.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("degraded job never timed out")
	}

	met := jm.Metrics()
	if met.Retransmits != 0 || met.RetransmitsFailed != 1 {
		t.Errorf("retransmit counters = %d / %d failed, want 0 / 1", met.Retransmits, met.RetransmitsFailed)
	}
	if sent := len(sender.snapshot()); sent != k {
		t.Errorf("job sent %d packets, want only the first attempt's %d", sent, k)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after final timeout: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after final timeout", surbs.Len())
	}
}

func TestFailedReplyStatusIsReported(t *testing.T) {
	sender := &fakeSender{}
	jm, _ := newTestJobManager(t, sender, jobSampleSize(1, ReturnPathsPerJob), JobConfig{Branches: 1})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	payload, err := EncodeReply(ReplyStatusFailed, nil)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], payload)

	got := <-res
	if !errors.Is(got.Err, ErrDiscoveryFailed) {
		t.Fatalf("failed reply settled with %v, want ErrDiscoveryFailed", got.Err)
	}
	if met := jm.Metrics(); met.JobsFailed != 1 {
		t.Errorf("JobsFailed = %d, want 1", met.JobsFailed)
	}
}

func TestUndecodableReplyDiscardsOnlyThatBranch(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, 1 // one SURB per branch: the garbage reply names one branch
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	ids := jm.pendingSURBIDs()
	if len(ids) != k*m {
		t.Fatalf("job registered %d surb ids, want %d", len(ids), k*m)
	}

	// One branch's proxy answers garbage: that contribution is discarded,
	// the job stays pending on the other branch
	jm.HandleSURBReply(ids[0], []byte("garbage"))
	select {
	case got := <-res:
		t.Fatalf("undecodable reply settled the job: %+v", got)
	default:
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 1 || surbIDs != k*m-1 {
		t.Errorf("state after discard: %d jobs / %d surb ids, want 1 / %d", jobs, surbIDs, k*m-1)
	}
	if met := jm.Metrics(); met.RepliesInvalid != 1 {
		t.Errorf("RepliesInvalid = %d, want 1", met.RepliesInvalid)
	}

	// The other branch settles the job normally
	want := testProviders(t, 2)
	payload, err := EncodeReply(ReplyStatusOK, want)
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(ids[1], payload)
	got, ok := <-res
	if !ok || got.Err != nil {
		t.Fatalf("job settled with ok=%v err=%v, want success via the healthy branch", ok, got.Err)
	}
	assertProvidersEqual(t, got.Providers, want)
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after settle", surbs.Len())
	}
}

func TestStartJobFailsOnSmallPool(t *testing.T) {
	sender := &fakeSender{}
	k, m := DefaultBranchesPerJob, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m)-1, JobConfig{})

	if _, err := jm.StartJob(context.Background(), testCID(t)); !errors.Is(err, ErrPoolTooSmall) {
		t.Fatalf("StartJob on a small pool: err = %v, want ErrPoolTooSmall", err)
	}
	if len(sender.snapshot()) != 0 {
		t.Error("a failed job start sent packets")
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("failed start left state: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("failed start left %d surb entries", surbs.Len())
	}
}

func TestStartJobAllBranchSendsFailCleansUp(t *testing.T) {
	sender := &fakeSender{err: errors.New("dial refused")}
	k, m := 2, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err == nil {
		t.Fatal("StartJob succeeded although every branch send failed")
	}
	met := jm.Metrics()
	if met.SendFailures != uint64(k) || met.JobsStarted != 0 {
		t.Errorf("counters = %d send failures / %d started, want %d / 0", met.SendFailures, met.JobsStarted, k)
	}
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("send failure left state: %d jobs / %d surb ids", jobs, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("send failure left %d surb entries", surbs.Len())
	}
}

func TestCountersHappyPath(t *testing.T) {
	sender := &fakeSender{}
	k, m := DefaultBranchesPerJob, ReturnPathsPerJob
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	ids := jm.pendingSURBIDs()
	payload, err := EncodeReply(ReplyStatusOK, testProviders(t, 3))
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(ids[0], payload)
	if got := <-res; got.Err != nil {
		t.Fatalf("job settled with error: %v", got.Err)
	}
	jm.HandleSURBReply(ids[1], payload) // cross-branch duplicate

	want := JobMetricsSnapshot{
		JobsStarted:       1,
		BranchPacketsSent: uint64(k),
		RepliesWon:        1,
		RepliesDuplicate:  1,
		JobsSucceeded:     1,
		ProxyCPLCount:     uint64(k),
	}
	got := jm.Metrics()
	// The achieved proxy CPL is a property of the random draw, not of the
	// code path: pin only its bound and the count, then compare the rest
	// strictly. TestProxyCPLCountersRecorded pins the exact sum
	if got.ProxyCPLSum > 256*got.ProxyCPLCount {
		t.Errorf("ProxyCPLSum = %d exceeds 256 per proxy (count %d)", got.ProxyCPLSum, got.ProxyCPLCount)
	}
	want.ProxyCPLSum = got.ProxyCPLSum
	if got != want {
		t.Errorf("happy-path counters:\n got %+v\nwant %+v", got, want)
	}
}

func TestCountersRetransmitPath(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, 2
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m, Timeout: 100 * time.Millisecond})

	res, err := jm.StartJob(context.Background(), testCID(t))
	if err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	select {
	case got := <-res:
		if !errors.Is(got.Err, ErrJobTimeout) {
			t.Fatalf("job settled with %v, want ErrJobTimeout", got.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never timed out")
	}

	want := JobMetricsSnapshot{
		JobsStarted:       1,
		BranchPacketsSent: uint64(2 * k),
		Retransmits:       1,
		JobsTimedOut:      1,
		ProxyCPLCount:     uint64(2 * k), // k proxies per registered attempt, two attempts
	}
	got := jm.Metrics()
	if got.ProxyCPLSum > 256*got.ProxyCPLCount {
		t.Errorf("ProxyCPLSum = %d exceeds 256 per proxy (count %d)", got.ProxyCPLSum, got.ProxyCPLCount)
	}
	want.ProxyCPLSum = got.ProxyCPLSum
	if got != want {
		t.Errorf("retransmit-path counters:\n got %+v\nwant %+v", got, want)
	}
}

func TestCloseSettlesPendingJobs(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, ReturnPathsPerJob
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, Timeout: 30 * time.Second})

	const jobs = 3
	results := make([]<-chan JobResult, jobs)
	for i := range results {
		res, err := jm.StartJob(context.Background(), testCID(t))
		if err != nil {
			t.Fatalf("StartJob %d: %v", i, err)
		}
		results[i] = res
	}

	jm.Close()
	for i, res := range results {
		got, ok := <-res
		if !ok {
			t.Fatalf("job %d: channel closed without a result", i)
		}
		if !errors.Is(got.Err, ErrManagerClosed) {
			t.Errorf("job %d settled with %v, want ErrManagerClosed", i, got.Err)
		}
	}
	if met := jm.Metrics(); met.JobsCanceled != jobs {
		t.Errorf("JobsCanceled = %d, want %d", met.JobsCanceled, jobs)
	}
	if pending, surbIDs := jm.pendingCounts(); pending != 0 || surbIDs != 0 {
		t.Errorf("state after Close: %d jobs / %d surb ids", pending, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after Close", surbs.Len())
	}

	// A closed manager rejects new jobs; a second Close is a no-op
	if _, err := jm.StartJob(context.Background(), testCID(t)); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("StartJob after Close: err = %v, want ErrManagerClosed", err)
	}
	jm.Close()
}

func TestCloseRacesRetransmit(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	jm, surbs := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m, Timeout: 30 * time.Millisecond})

	const jobs = 15
	results := make([]<-chan JobResult, jobs)
	for i := range results {
		res, err := jm.StartJob(context.Background(), testCID(t))
		if err != nil {
			t.Fatalf("StartJob %d: %v", i, err)
		}
		results[i] = res
	}

	// Close lands around the retransmit window: some retransmits have
	// committed, some abandon at their re-lock, some timers fire into a
	// missing job. Every channel must settle exactly once either way, and
	// the SURB store must drain completely
	time.Sleep(25 * time.Millisecond)
	jm.Close()

	for i, res := range results {
		select {
		case got, ok := <-res:
			if !ok {
				t.Fatalf("job %d: channel closed without a result", i)
			}
			if got.Err == nil {
				t.Errorf("job %d settled without error despite no replies", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("job %d never settled", i)
		}
		if _, ok := <-res; ok {
			t.Errorf("job %d produced a second result", i)
		}
	}
	if pending, surbIDs := jm.pendingCounts(); pending != 0 || surbIDs != 0 {
		t.Errorf("state after Close: %d jobs / %d surb ids", pending, surbIDs)
	}
	if surbs.Len() != 0 {
		t.Errorf("surb store holds %d entries after Close", surbs.Len())
	}
}

// --- ℓ-biased proxy selection ---

// fillPoolMatching fills pool with n records whose keyspace positions
// satisfy match relative to target. Rejection sampling: a random position
// passes CPL >= ℓ with probability 2^-ℓ, so only usable for small ℓ
func fillPoolMatching(t *testing.T, pool *KeyStore, target kb.ID, n int, match func(cpl int) bool) []peer.ID {
	t.Helper()
	ids := make([]peer.ID, 0, n)
	for len(ids) < n {
		idPriv, pid := newIdentity(t)
		if !match(kb.CommonPrefixLen(kb.ConvertPeerID(pid), target)) {
			continue
		}
		km, err := NewKeyManager(idPriv, time.Hour)
		if err != nil {
			t.Fatalf("NewKeyManager: %v", err)
		}
		if err := pool.Put(NewKeyRecord(pid, km.PublicKey(), time.Hour)); err != nil {
			t.Fatalf("pool.Put: %v", err)
		}
		ids = append(ids, pid)
	}
	return ids
}

// drawProxies returns the proxy of every branch group in draw: the exit
// slot bi·g + NrHops−1 that buildBranches reads
func drawProxies(draw []KeyInfo, k, g int) []KeyInfo {
	proxies := make([]KeyInfo, k)
	for bi := range k {
		proxies[bi] = draw[bi*g+NrHops-1]
	}
	return proxies
}

// nearestPeers returns the n peers of infos nearest to target by XOR
// distance, as a set
func nearestPeers(infos []KeyInfo, target kb.ID, n int) map[peer.ID]struct{} {
	sorted := append([]KeyInfo(nil), infos...)
	sort.Slice(sorted, func(a, b int) bool {
		da := kb.Xor(kb.ConvertPeerID(sorted[a].PeerID), target)
		db := kb.Xor(kb.ConvertPeerID(sorted[b].PeerID), target)
		return bytes.Compare(da, db) < 0
	})
	out := make(map[peer.ID]struct{}, n)
	for _, ki := range sorted[:n] {
		out[ki.PeerID] = struct{}{}
	}
	return out
}

// assertDrawDistinct fails when draw holds any peer twice: proxies and
// relays of one attempt must be pairwise disjoint
func assertDrawDistinct(t *testing.T, draw []KeyInfo) {
	t.Helper()
	seen := make(map[peer.ID]struct{}, len(draw))
	for _, ki := range draw {
		if _, dup := seen[ki.PeerID]; dup {
			t.Errorf("draw holds %s twice", ki.PeerID)
		}
		seen[ki.PeerID] = struct{}{}
	}
}

func TestNewJobManagerRejectsBadMinProxyCPL(t *testing.T) {
	idPriv, _ := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	for _, bad := range []int{-1, 257} {
		if _, err := NewJobManager(&fakeSender{}, km, NewKeyStore(), NewSURBStore(), JobConfig{MinProxyCPL: bad}); err == nil {
			t.Errorf("NewJobManager accepted MinProxyCPL = %d", bad)
		}
	}
}

// ℓ = 0 short-circuits to one uniform SampleExcluding over the pool; the
// rest of the suite runs at that default and guards the equivalence.
// Pinned here: full pool consumed pairwise distinct, exclusion honored,
// shortfall reported rather than padded
func TestDrawAttemptLZeroIsUniformDraw(t *testing.T) {
	k, m := 2, 1
	need := jobSampleSize(k, m)
	jm, _ := newTestJobManager(t, &fakeSender{}, need, JobConfig{Branches: k, ReturnPaths: m})

	draw := jm.drawAttempt(testCID(t), nil)
	if len(draw) != need {
		t.Fatalf("draw has %d relays, want %d", len(draw), need)
	}
	assertDrawDistinct(t, draw)

	exclude := map[peer.ID]struct{}{draw[0].PeerID: {}}
	short := jm.drawAttempt(testCID(t), exclude)
	if len(short) != need-1 {
		t.Fatalf("excluded draw has %d relays, want the shortfall %d", len(short), need-1)
	}
	for _, ki := range short {
		if _, ok := exclude[ki.PeerID]; ok {
			t.Errorf("excluded peer %s drawn anyway", ki.PeerID)
		}
	}
}

// TestDrawAttemptL256PicksKNearest pins the upper degenerate case: at
// ℓ = 256 no real peer passes the threshold, so the fill rule alone
// chooses: exactly the k known peers nearest to the CID's provider key by
// XOR distance
func TestDrawAttemptL256PicksKNearest(t *testing.T) {
	k, m := 2, 1
	need := jobSampleSize(k, m)
	jm, _ := newTestJobManager(t, &fakeSender{}, 3*need, JobConfig{Branches: k, ReturnPaths: m, MinProxyCPL: 256})
	c := testCID(t)
	target := providerKeyID(c)
	want := nearestPeers(jm.pool.Sample(jm.pool.Len()), target, k)

	draw := jm.drawAttempt(c, nil)
	if len(draw) != need {
		t.Fatalf("draw has %d relays, want %d", len(draw), need)
	}
	assertDrawDistinct(t, draw)
	for _, p := range drawProxies(draw, k, jm.branchGroupSize()) {
		if _, ok := want[p.PeerID]; !ok {
			t.Errorf("proxy %s is not among the %d nearest peers", p.PeerID, k)
		}
	}
}

// TestDrawAttemptThresholdAndFill covers the mid-range rule at a small ℓ
// (rejection-sampled identities, see fillPoolMatching): with more than k
// threshold passers every proxy passes; with fewer, all passers are chosen
// and the rest is filled with the nearest non-passers by XOR distance
func TestDrawAttemptThresholdAndFill(t *testing.T) {
	const l = 2
	k, m := 2, 1
	need := jobSampleSize(k, m)
	c := testCID(t)
	target := providerKeyID(c)
	passes := func(cpl int) bool { return cpl >= l }
	fails := func(cpl int) bool { return cpl < l }

	newBiasedManager := func(pool *KeyStore) *JobManager {
		idPriv, _ := newIdentity(t)
		km, err := NewKeyManager(idPriv, time.Hour)
		if err != nil {
			t.Fatalf("NewKeyManager: %v", err)
		}
		jm, err := NewJobManager(&fakeSender{}, km, pool, NewSURBStore(), JobConfig{Branches: k, ReturnPaths: m, MinProxyCPL: l})
		if err != nil {
			t.Fatalf("NewJobManager: %v", err)
		}
		return jm
	}

	// More passers than k: every chosen proxy passes the threshold
	pool := NewKeyStore()
	passers := fillPoolMatching(t, pool, target, k+2, passes)
	fillPoolMatching(t, pool, target, need, fails)
	passerSet := make(map[peer.ID]struct{}, len(passers))
	for _, pid := range passers {
		passerSet[pid] = struct{}{}
	}
	jm := newBiasedManager(pool)
	draw := jm.drawAttempt(c, nil)
	if len(draw) != need {
		t.Fatalf("draw has %d relays, want %d", len(draw), need)
	}
	assertDrawDistinct(t, draw)
	for _, p := range drawProxies(draw, k, jm.branchGroupSize()) {
		if _, ok := passerSet[p.PeerID]; !ok {
			t.Errorf("proxy %s does not pass the cpl ≥ %d threshold", p.PeerID, l)
		}
	}

	// One passer only: it is always chosen, and the second proxy is the
	// nearest non-passer by XOR distance
	pool = NewKeyStore()
	passer := fillPoolMatching(t, pool, target, 1, passes)[0]
	fillPoolMatching(t, pool, target, need+3, fails)
	var nonPassers []KeyInfo
	for _, ki := range pool.Sample(pool.Len()) {
		if ki.PeerID != passer {
			nonPassers = append(nonPassers, ki)
		}
	}
	fill := nearestPeers(nonPassers, target, 1)
	jm = newBiasedManager(pool)
	draw = jm.drawAttempt(c, nil)
	if len(draw) != need {
		t.Fatalf("draw has %d relays, want %d", len(draw), need)
	}
	assertDrawDistinct(t, draw)
	gotProxies := make(map[peer.ID]struct{}, k)
	for _, p := range drawProxies(draw, k, jm.branchGroupSize()) {
		gotProxies[p.PeerID] = struct{}{}
	}
	if _, ok := gotProxies[passer]; !ok {
		t.Errorf("the sole threshold passer %s was not chosen", passer)
	}
	for pid := range fill {
		if _, ok := gotProxies[pid]; !ok {
			t.Errorf("the nearest non-passer %s did not fill the proxy set", pid)
		}
	}
}

// biased draw under the retransmit's exclusion: no excluded peer appears,
// and the one threshold passer outside the exclusion is chosen ahead of
// any fill. The exclusion is constructed, not drawn
func TestDrawAttemptBiasedHonorsExclusion(t *testing.T) {
	const l = 2
	k, m := 1, 1
	need := jobSampleSize(k, m)
	c := testCID(t)
	target := providerKeyID(c)

	idPriv, _ := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	pool := NewKeyStore()
	passers := fillPoolMatching(t, pool, target, 2, func(cpl int) bool { return cpl >= l })
	nonPassers := fillPoolMatching(t, pool, target, 2*need, func(cpl int) bool { return cpl < l })

	jm, err := NewJobManager(&fakeSender{}, km, pool, NewSURBStore(), JobConfig{Branches: k, ReturnPaths: m, MinProxyCPL: l})
	if err != nil {
		t.Fatalf("NewJobManager: %v", err)
	}

	// Exclude one full attempt's worth of peers: the first passer plus
	// need−1 non-passers. Exactly one passer survives
	exclude := map[peer.ID]struct{}{passers[0]: {}}
	for _, pid := range nonPassers[:need-1] {
		exclude[pid] = struct{}{}
	}

	draw := jm.drawAttempt(c, exclude)
	if len(draw) != need {
		t.Fatalf("excluded draw has %d relays, want %d", len(draw), need)
	}
	assertDrawDistinct(t, draw)
	for _, ki := range draw {
		if _, ok := exclude[ki.PeerID]; ok {
			t.Errorf("excluded peer %s drawn anyway", ki.PeerID)
		}
	}
	if got := drawProxies(draw, k, jm.branchGroupSize())[0].PeerID; got != passers[1] {
		t.Errorf("excluded draw chose proxy %s, want the remaining passer %s", got, passers[1])
	}
}

// TestBiasedDrawStaysFullyDisjoint runs the full StartJob path at ℓ > 0
// with the pool exactly one attempt's demand: threshold or fill, the
// biased draw must still consume every live entry exactly once, the
// ℓ-chosen proxies and all uniformly drawn relays pairwise disjoint
func TestBiasedDrawStaysFullyDisjoint(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, ReturnPathsPerJob
	need := jobSampleSize(k, m)
	jm, _ := newTestJobManager(t, sender, need, JobConfig{Branches: k, MinProxyCPL: 1})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if relays := jm.pendingJobRelays(t); len(relays) != need {
		t.Errorf("job drew %d distinct relays, want %d (fully disjoint)", len(relays), need)
	}
}

// TestRetransmitPrefersFreshRelaysUnderBias is the ℓ > 0 face of
// TestRetransmitPrefersFreshRelays: with the pool at twice one attempt's
// demand, the biased retransmit draw must avoid every first-attempt relay
// entirely; the exclusion applies to proxy and relay stages alike
func TestRetransmitPrefersFreshRelaysUnderBias(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	need := jobSampleSize(k, m)
	jm, _ := newTestJobManager(t, sender, 2*need, JobConfig{Branches: k, ReturnPaths: m, Timeout: 300 * time.Millisecond, MinProxyCPL: 2})

	if _, err := jm.StartJob(context.Background(), testCID(t)); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	waitFor(t, 5*time.Second, "the retransmit wave", func() bool { return jm.Metrics().Retransmits == 1 })

	if relays := jm.pendingJobRelays(t); len(relays) != 2*need {
		t.Errorf("job drew %d distinct relays over two attempts, want %d (fully disjoint)", len(relays), 2*need)
	}
}

// TestProxyCPLCountersRecorded pins the achieved-proximity counters at the
// deterministic endpoint: ℓ = 256 chooses exactly the k nearest peers, so
// the recorded sum is computable from the pool
func TestProxyCPLCountersRecorded(t *testing.T) {
	sender := &fakeSender{}
	k, m := 2, 1
	jm, _ := newTestJobManager(t, sender, 3*jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m, MinProxyCPL: 256})
	c := testCID(t)
	target := providerKeyID(c)

	var wantSum uint64
	for pid := range nearestPeers(jm.pool.Sample(jm.pool.Len()), target, k) {
		wantSum += uint64(kb.CommonPrefixLen(kb.ConvertPeerID(pid), target))
	}

	if _, err := jm.StartJob(context.Background(), c); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	met := jm.Metrics()
	if met.ProxyCPLCount != uint64(k) || met.ProxyCPLSum != wantSum {
		t.Errorf("proxy cpl counters = sum %d / count %d, want %d / %d", met.ProxyCPLSum, met.ProxyCPLCount, wantSum, k)
	}
}
