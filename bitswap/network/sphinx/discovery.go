package sphinx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
)

// DefaultJobTimeout bounds one attempt; a job retransmits once, so worst
// case is twice this. Must exceed the proxy timeout plus path round trips.
// Placeholder until calibration
const DefaultJobTimeout = 30 * time.Second

// DefaultBranchesPerJob is k. 2 survives one dead branch without a
// retransmit at a pool demand (18) small pools can still carry
const DefaultBranchesPerJob = 2

var ErrJobTimeout = errors.New("discovery job timed out")

// ErrDiscoveryFailed is ReplyStatusFailed at the caller; no automatic
// fallback to vanilla discovery
var ErrDiscoveryFailed = errors.New("proxy reported discovery failure")

var ErrPoolTooSmall = errors.New("relay pool cannot supply a full job sample")

var ErrManagerClosed = errors.New("job manager is closed")

// JobResult is what a job settles to; exactly one field is set
type JobResult struct {
	Providers []peer.AddrInfo
	Err       error
}

type JobConfig struct {
	// Timeout bounds one attempt; zero means DefaultJobTimeout
	Timeout time.Duration
	// ReturnPaths is m, SURBs per branch; zero means ReturnPathsPerJob.
	// Bounded by the payload budget, not the geometry
	ReturnPaths int
	// Branches is k; zero means DefaultBranchesPerJob. Never on the wire,
	// only scales the pool demand k·(NrHops+2m)
	Branches int
	// MinProxyCPL is ℓ (0..256): minimum CPL between branch proxies and
	// the CID's provider key. 0 = uniform draw; higher values bias proxies
	// toward the CID's DHT neighborhood and leak its keyspace region to
	// the last forward relay. See drawAttempt
	MinProxyCPL int
	// DisableRetransmit limits a job to a single attempt (evaluation seam)
	DisableRetransmit bool
}

// JobManager is the initiator role: launches jobs as k branches,
// retransmits once on attempt timeout, settles jobs from SURB replies.
// One timer and one SURB ledger (<= 2·k·m IDs) per job; every settle path
// deletes the job with all its store entries
type JobManager struct {
	sender       PacketSender
	km           *KeyManager
	pool         *KeyStore
	surbs        *SURBStore
	timeout      time.Duration
	m            int
	k            int
	minCPL       int
	noRetransmit bool

	metrics JobMetrics

	mu     sync.Mutex
	closed bool
	jobs   map[uint64]*discoveryJob
	bySURB map[SURBID]uint64
	nextID uint64
}

type discoveryJob struct {
	id  uint64
	cid cid.Cid
	// 1 until the retransmit commits, then 2; guarded by mu
	attempt int
	// every relay drawn for this job, excluded by the retransmit draw.
	// Written under mu at registration; the retransmit callback is the
	// only later writer
	relays map[peer.ID]struct{}
	// append-only ledger of all SURB IDs, both attempts; scrub deletes
	// exactly this set when the job ends
	surbIDs []SURBID
	// the currently armed attempt timer; the retransmit swaps it under mu
	timer  *time.Timer
	result chan JobResult
}

// branch is one built forward path: packet for its first hop plus the SURB
// material of its m return paths
type branch struct {
	firstHop peer.ID
	pkt      []byte
	surbIDs  []SURBID
	surbKeys [][]byte
}

func NewJobManager(sender PacketSender, km *KeyManager, pool *KeyStore, surbs *SURBStore, cfg JobConfig) (*JobManager, error) {
	if sender == nil || km == nil || pool == nil || surbs == nil {
		return nil, errors.New("job manager needs a sender, key manager, key store and surb store")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultJobTimeout
	}
	if cfg.ReturnPaths == 0 {
		cfg.ReturnPaths = ReturnPathsPerJob
	}
	if cfg.Branches == 0 {
		cfg.Branches = DefaultBranchesPerJob
	}
	if cfg.Timeout < 0 || cfg.ReturnPaths < 0 || cfg.ReturnPaths > 255 || cfg.Branches < 0 || cfg.Branches > 255 {
		return nil, errors.New("job timeout must be positive, return paths and branches in 1..255")
	}
	if cfg.MinProxyCPL < 0 || cfg.MinProxyCPL > 256 {
		return nil, errors.New("min proxy cpl must be in 0..256")
	}
	return &JobManager{
		sender:       sender,
		km:           km,
		pool:         pool,
		surbs:        surbs,
		timeout:      cfg.Timeout,
		m:            cfg.ReturnPaths,
		k:            cfg.Branches,
		minCPL:       cfg.MinProxyCPL,
		noRetransmit: cfg.DisableRetransmit,
		jobs:         make(map[uint64]*discoveryJob),
		bySURB:       make(map[SURBID]uint64),
	}, nil
}

func (jm *JobManager) Metrics() JobMetricsSnapshot {
	return jm.metrics.Snapshot()
}

// forward path (NrHops incl. proxy) + 2 relays per return path
func (jm *JobManager) branchGroupSize() int { return NrHops + 2*jm.m }

// pool demand of one attempt
func (jm *JobManager) sampleSize() int { return jm.k * jm.branchGroupSize() }

// StartJob launches provider discovery for c: k branches, each a forward
// path to its own proxy plus m return paths, drawn in one sample so all
// relays are distinct. The buffered channel receives exactly one JobResult
// and is closed. No reply within the timeout triggers one retransmit over
// fresh branches. ctx bounds only the first attempt's sends
func (jm *JobManager) StartJob(ctx context.Context, c cid.Cid) (<-chan JobResult, error) {
	need := jm.sampleSize()
	draw := jm.drawAttempt(c, nil)
	if len(draw) < need {
		return nil, fmt.Errorf("%w: need %d distinct relays, pool has %d live", ErrPoolTooSmall, need, len(draw))
	}
	branches, err := jm.buildBranches(c, draw)
	if err != nil {
		return nil, err
	}

	// register and arm the timer before sending: a reply can arrive while
	// SendPacket still blocks
	j := &discoveryJob{
		cid:     c,
		attempt: 1,
		relays:  make(map[peer.ID]struct{}, 2*need),
		result:  make(chan JobResult, 1),
	}
	jm.mu.Lock()
	if jm.closed {
		jm.mu.Unlock()
		return nil, ErrManagerClosed
	}
	jm.nextID++
	j.id = jm.nextID
	jm.jobs[j.id] = j
	jm.registerAttemptLocked(j, draw, branches)
	jm.mu.Unlock()

	sent := 0
	for _, b := range branches {
		if jm.sendBranch(ctx, b) {
			sent++
		}
	}
	if sent == 0 {
		// no packet in flight, no reply can come; fail fast
		if j, ok := jm.removeByJob(j.id); ok {
			jm.scrub(j)
		}
		return nil, fmt.Errorf("sending discovery job: all %d branch sends failed", len(branches))
	}
	jm.metrics.JobsStarted.Add(1)
	return j.result, nil
}

// Close settles every pending job with ErrManagerClosed and rejects later
// StartJob calls; idempotent
func (jm *JobManager) Close() {
	jm.mu.Lock()
	if jm.closed {
		jm.mu.Unlock()
		return
	}
	jm.closed = true
	pending := make([]*discoveryJob, 0, len(jm.jobs))
	for _, j := range jm.jobs {
		pending = append(pending, j)
	}
	for _, j := range pending {
		jm.unregisterLocked(j)
	}
	jm.mu.Unlock()

	for _, j := range pending {
		jm.scrub(j)
		jm.metrics.JobsCanceled.Add(1)
		j.result <- JobResult{Err: ErrManagerClosed}
		close(j.result)
	}
}

// buildBranches splits draw into k groups and builds one branch per group.
// Sampling without replacement already gives the path invariants: no proxy
// on its own return paths, disjoint return paths, first forward relay !=
// last return relay
func (jm *JobManager) buildBranches(c cid.Cid, draw []KeyInfo) ([]branch, error) {
	self := KeyInfo{PeerID: jm.km.PeerID(), PublicKey: jm.km.PublicKey()}
	g := jm.branchGroupSize()
	branches := make([]branch, jm.k)
	for bi := range branches {
		group := draw[bi*g : (bi+1)*g]
		// group[0..NrHops-2] forward relays, group[NrHops-1] the proxy
		forward := group[:NrHops]
		surbs := make([][]byte, jm.m)
		surbIDs := make([]SURBID, jm.m)
		surbKeys := make([][]byte, jm.m)
		for i := range jm.m {
			hops := []KeyInfo{group[NrHops+2*i], group[NrHops+2*i+1], self}
			surb, id, keys, err := NewSURB(hops)
			if err != nil {
				return nil, fmt.Errorf("branch %d: building surb %d: %w", bi, i, err)
			}
			surbs[i], surbIDs[i], surbKeys[i] = surb, id, keys
		}
		payload, err := EncodeJob(c, surbs)
		if err != nil {
			return nil, err
		}
		pkt, err := NewForwardPacket(forward, DiscoveryRecipient, payload)
		if err != nil {
			return nil, err
		}
		branches[bi] = branch{firstHop: forward[0].PeerID, pkt: pkt, surbIDs: surbIDs, surbKeys: surbKeys}
	}
	return branches, nil
}

// drawAttempt draws the k·(NrHops+2m) relays of one attempt, minus exclude.
// A short draw means pool shortfall; the caller decides. minCPL = 0 is one
// uniform sample. Otherwise only the proxy slots are biased
// (selectProxies); the other relays are a fresh uniform draw around them,
// keeping the attempt disjoint and the relay positions unbiased
func (jm *JobManager) drawAttempt(c cid.Cid, exclude map[peer.ID]struct{}) []KeyInfo {
	need := jm.sampleSize()
	if jm.minCPL == 0 {
		return jm.pool.SampleExcluding(need, exclude)
	}

	// full-length draw = all live candidates, already shuffled; makes
	// selectProxies' "first k passers" uniform within the threshold set
	live := jm.pool.SampleExcluding(jm.pool.Len(), exclude)
	if len(live) < need {
		return live
	}
	proxies := selectProxies(live, jm.k, jm.minCPL, providerKeyID(c))

	exclProxies := make(map[peer.ID]struct{}, len(exclude)+len(proxies))
	for pid := range exclude {
		exclProxies[pid] = struct{}{}
	}
	for _, p := range proxies {
		exclProxies[p.PeerID] = struct{}{}
	}
	relays := jm.pool.SampleExcluding(need-jm.k, exclProxies)
	if len(relays) < need-jm.k {
		// entries expired between the two draws; report the shortfall
		return relays
	}

	// compose with each branch's proxy at its exit slot
	g := jm.branchGroupSize()
	draw := make([]KeyInfo, 0, need)
	for bi := range jm.k {
		draw = append(draw, relays[:NrHops-1]...)
		draw = append(draw, proxies[bi])
		draw = append(draw, relays[NrHops-1:g-1]...)
		relays = relays[g-1:]
	}
	return draw
}

// providerKeyID: SHA-256 over the CID's multihash, the preimage kad-dht
// hashes for FindProvidersAsync (CIDv0/v1 share one position)
func providerKeyID(c cid.Cid) kb.ID {
	return kb.ConvertKey(string(c.Hash()))
}

// selectProxies picks k proxies from live (all live candidates in random
// order, >= k entries): the first k with CPL >= minCPL to target, topped
// up with the nearest remaining by XOR distance if fewer pass. minCPL =
// 256 therefore yields the k nearest known peers
func selectProxies(live []KeyInfo, k, minCPL int, target kb.ID) []KeyInfo {
	chosen := make([]KeyInfo, 0, k)
	var rest []KeyInfo
	var restDist []kb.ID
	for _, ki := range live {
		id := kb.ConvertPeerID(ki.PeerID)
		if len(chosen) < k && kb.CommonPrefixLen(id, target) >= minCPL {
			chosen = append(chosen, ki)
			continue
		}
		rest = append(rest, ki)
		restDist = append(restDist, kb.Xor(id, target))
	}
	if fill := k - len(chosen); fill > 0 {
		idx := make([]int, len(rest))
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool {
			return bytes.Compare(restDist[idx[a]], restDist[idx[b]]) < 0
		})
		for _, i := range idx[:fill] {
			chosen = append(chosen, rest[i])
		}
	}
	return chosen
}

// registerAttemptLocked adds one attempt: SURB ledger, reply map, store
// keys, relay set, timer. Map registration and store Put must share the
// critical section (lock order mu -> SURBStore.mu): a reply can settle the
// job at any instant, and a Put after the settling scrub would leak
func (jm *JobManager) registerAttemptLocked(j *discoveryJob, draw []KeyInfo, branches []branch) {
	for _, b := range branches {
		for i, id := range b.surbIDs {
			j.surbIDs = append(j.surbIDs, id)
			jm.bySURB[id] = j.id
			jm.surbs.Put(id, b.surbKeys[i])
		}
	}
	for _, ki := range draw {
		j.relays[ki.PeerID] = struct{}{}
	}
	jm.recordProxyCPLs(j.cid, draw)
	j.timer = time.AfterFunc(jm.timeout, func() { jm.onTimer(j.id) })
}

// recordProxyCPLs: per branch proxy the CPL to the CID's provider key,
// recorded at every attempt (ℓ = 0 included, as baseline)
func (jm *JobManager) recordProxyCPLs(c cid.Cid, draw []KeyInfo) {
	target := providerKeyID(c)
	g := jm.branchGroupSize()
	for bi := range jm.k {
		proxy := draw[bi*g+NrHops-1]
		cpl := kb.CommonPrefixLen(kb.ConvertPeerID(proxy.PeerID), target)
		jm.metrics.ProxyCPLSum.Add(uint64(cpl))
		jm.metrics.ProxyCPLCount.Add(1)
	}
}

// a failed send only kills the branch
func (jm *JobManager) sendBranch(ctx context.Context, b branch) bool {
	if err := jm.sender.SendPacket(ctx, b.firstHop, b.pkt); err != nil {
		jm.metrics.SendFailures.Add(1)
		log.Debugw("branch packet dropped", "first", b.firstHop, "error", err)
		return false
	}
	jm.metrics.BranchPacketsSent.Add(1)
	return true
}

// HandleSURBReply is the single arbitration site: first decodable reply
// wins, across all branches and attempts; a Failed reply settles the job
// too. An undecodable reply drops only that SURB: it can only come from
// that branch's proxy (payload authenticated with initiator-held keys),
// and one bad proxy must not kill a job another branch would settle
func (jm *JobManager) HandleSURBReply(id SURBID, payload []byte) {
	reply, err := DecodeReply(payload)
	if err != nil {
		jm.metrics.RepliesInvalid.Add(1)
		jm.dropSURB(id)
		log.Debugw("discarding undecodable surb reply", "surb", fmt.Sprintf("%x", id), "error", err)
		return
	}
	j, ok := jm.takeBySURB(id)
	if !ok {
		// duplicate of a settled job, or never ours
		jm.metrics.RepliesDuplicate.Add(1)
		log.Debugw("ignoring surb reply without a pending job", "surb", fmt.Sprintf("%x", id))
		return
	}
	jm.scrub(j)
	jm.metrics.RepliesWon.Add(1)

	var res JobResult
	if reply.Status == ReplyStatusFailed {
		res.Err = ErrDiscoveryFailed
		jm.metrics.JobsFailed.Add(1)
	} else {
		res.Providers = reply.Providers
		jm.metrics.JobsSucceeded.Add(1)
	}
	j.result <- res
	close(j.result)
}

// first timer fires the retransmit, second settles as ErrJobTimeout
func (jm *JobManager) onTimer(jobID uint64) {
	jm.mu.Lock()
	j, ok := jm.jobs[jobID]
	if !ok {
		jm.mu.Unlock()
		return
	}
	if j.attempt == 1 && !jm.noRetransmit {
		jm.mu.Unlock()
		jm.retransmit(j)
		return
	}
	jm.unregisterLocked(j)
	jm.mu.Unlock()
	jm.scrub(j)
	jm.metrics.JobsTimedOut.Add(1)
	j.result <- JobResult{Err: ErrJobTimeout}
	close(j.result)
}

// retransmit is the second and final attempt: fresh draw (avoiding the
// first attempt's relays when the pool allows), k new branches, one final
// timer. First-attempt SURBs stay registered so a late reply still wins.
// If even the unrestricted draw is short, no second wave; the final timer
// covers the first attempt's SURBs alone
func (jm *JobManager) retransmit(j *discoveryJob) {
	need := jm.sampleSize()
	draw := jm.drawAttempt(j.cid, j.relays)
	if len(draw) < need {
		draw = jm.drawAttempt(j.cid, nil)
	}
	var branches []branch
	var err error
	if len(draw) < need {
		err = fmt.Errorf("%w: need %d distinct relays, pool has %d live", ErrPoolTooSmall, need, len(draw))
	} else {
		branches, err = jm.buildBranches(j.cid, draw)
	}

	jm.mu.Lock()
	if _, ok := jm.jobs[j.id]; !ok || jm.closed {
		// a reply or Close settled the job during the build; nothing of
		// this attempt was registered, nothing to undo
		jm.mu.Unlock()
		return
	}
	j.attempt = 2
	if err != nil {
		j.timer = time.AfterFunc(jm.timeout, func() { jm.onTimer(j.id) })
		jm.mu.Unlock()
		jm.metrics.RetransmitsFailed.Add(1)
		log.Debugw("retransmit degraded to waiting out the final timer", "cid", j.cid, "error", err)
		return
	}
	jm.registerAttemptLocked(j, draw, branches)
	jm.mu.Unlock()
	jm.metrics.Retransmits.Add(1)

	// the StartJob ctx bounded only the first attempt; even if every send
	// fails, the final timer settles the job
	for _, b := range branches {
		sctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
		jm.sendBranch(sctx, b)
		cancel()
	}
}

// takeBySURB claims the job for a reply: job and all its SURB mappings
// leave the maps in one critical section, so only the first reply wins
func (jm *JobManager) takeBySURB(id SURBID) (*discoveryJob, bool) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jobID, ok := jm.bySURB[id]
	if !ok {
		return nil, false
	}
	j := jm.jobs[jobID]
	jm.unregisterLocked(j)
	return j, true
}

// like takeBySURB, keyed by job ID
func (jm *JobManager) removeByJob(jobID uint64) (*discoveryJob, bool) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	j, ok := jm.jobs[jobID]
	if !ok {
		return nil, false
	}
	jm.unregisterLocked(j)
	return j, true
}

// caller holds mu
func (jm *JobManager) unregisterLocked(j *discoveryJob) {
	delete(jm.jobs, j.id)
	for _, sid := range j.surbIDs {
		delete(jm.bySURB, sid)
	}
}

// dropSURB discards one SURB's mapping and store entry without settling
// its job; the ID stays in the ledger, the final scrub's delete is a no-op
func (jm *JobManager) dropSURB(id SURBID) {
	jm.mu.Lock()
	delete(jm.bySURB, id)
	jm.mu.Unlock()
	jm.surbs.Delete(id)
}

// scrub stops the timer and deletes the job's whole SURB ledger, used or
// not. Callers own j exclusively (claimed via takeBySURB/removeByJob/
// Close), so no lock
func (jm *JobManager) scrub(j *discoveryJob) {
	if j.timer != nil {
		j.timer.Stop()
	}
	for _, sid := range j.surbIDs {
		jm.surbs.Delete(sid)
	}
}
