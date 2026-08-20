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
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// DefaultJobTimeout bounds one attempt; a job retransmits once, so worst
// case is twice this. Must exceed the proxy timeout plus path round trips.
// Placeholder until calibration
const DefaultJobTimeout = 30 * time.Second

// DefaultBranchesPerJob is k. 2 survives one dead branch without a
// retransmit, at a pool demand (18) small pools can still carry
const DefaultBranchesPerJob = 2

var ErrJobTimeout = errors.New("discovery job timed out")

// ErrDiscoveryFailed is ReplyStatusFailed at the caller; no automatic
// fallback to vanilla discovery
var ErrDiscoveryFailed = errors.New("proxy reported discovery failure")

// ErrSendFailed: every branch send of every attempt failed, so no packet
// was ever in flight and no reply was possible
var ErrSendFailed = errors.New("every branch send of the discovery job failed")

// ErrInvalidReplies: the branches answered, but exclusively with
// undecodable payloads; no proxy reported a result
var ErrInvalidReplies = errors.New("every reply of the discovery job was undecodable")

var ErrPoolTooSmall = errors.New("relay pool cannot supply a full job sample")

var ErrManagerClosed = errors.New("job manager is closed")

// ErrClientMode: StartJob was called on a node that does not serve keys.
// Only relay-set members initiate, so their first hop cannot tell
// origination from forwarding; a client-mode start would leak this node
// as the initiator
var ErrClientMode = errors.New("job manager will not initiate in client mode")

// JobResult is what a job settles to; exactly one field is set
type JobResult struct {
	Providers []peer.AddrInfo
	Err       error
}

// DrawPolicy selects how an attempt's relays come out of the pool
type DrawPolicy int

const (
	// DrawExclusive is the default: one sample without replacement makes
	// all relays of an attempt pairwise distinct, and the retransmit
	// excludes the first attempt's relays where the pool allows it
	DrawExclusive DrawPolicy = iota
	// DrawPerAttempt keeps the exclusive sample inside one attempt but
	// draws each attempt on its own: the retransmit takes a fresh
	// without-replacement sample and does not exclude the first attempt,
	// so the two attempts may overlap. It drops the only part of the
	// policy a relay can reason about across waves ("this job will not
	// draw me again") while keeping every path invariant. Evaluation arm
	DrawPerAttempt
	// DrawIndependent draws every relay slot as an independent uniform
	// pick with replacement. Only two rules survive: the k proxies stay
	// pairwise distinct, and no drawn node is asked to dial itself. The
	// retransmit repeats the same draw without excluding the first
	// attempt. Evaluation arm; see drawIndependent
	DrawIndependent
)

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
	// Draw selects the relay draw policy; the zero value DrawExclusive is
	// the without-replacement default. See DrawPolicy
	Draw DrawPolicy
	// InitiatorEdgeEpsilon steers the connection bias on relay slots whose
	// observed neighbor is the initiator (forward first hop and the last
	// relay of each return path): per such slot, the probability of a
	// connected peer while both connected and unconnected candidates
	// remain in the attempt's draw. 0 always prefers unconnected, 1 always
	// connected. Negative disables the rule (evaluation control); above 1
	// is rejected. The zero value 0.0 is the default bias. Evaluation
	// lever only; the design does not touch it
	InitiatorEdgeEpsilon float64
	// PeerState feeds the bias and its diagnostics; nil disables both.
	// NewService fills it from the host
	PeerState PeerState
	// ServingCheck reports whether this node currently serves keys, i.e.
	// runs as a relay. StartJob is refused when it returns false: only
	// relay-set members initiate, keeping an initiating node
	// indistinguishable from a forwarding one at its first hop. Read live
	// at every StartJob, so ModeAuto flips take effect at once. Nil
	// disables the gate, the ungated default for direct callers and tests;
	// NewService fills it from the key exchange
	ServingCheck func() bool
	// Observer receives attempt, branch and settle events of every job;
	// nil disables the seam. Evaluation lever only
	Observer JobObserver
}

// PeerState reads selection-time facts about relay candidates: connection
// state for the InitiatorEdgeEpsilon bias, peerstore addresses for the
// address diagnostic
type PeerState interface {
	Connectedness(p peer.ID) network.Connectedness
	Addrs(p peer.ID) []ma.Multiaddr
}

// JobManager is the initiator role: launches jobs as k branches, gives
// every branch until the attempt timer to answer, settles on the merged
// answers (mergeAnswers) or retransmits once when no branch produced an
// OK answer. One timer and at most k·m live SURB entries per job, since
// the retry replaces the first attempt's; every settle path deletes the
// job with all its store entries
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
	draw         DrawPolicy
	epsilon      float64
	state        PeerState
	// nil disables the gate; NewService fills it from the key exchange
	serving func() bool
	// crypto/rand by default; tests script it
	rand func(int) int
	obs  JobObserver

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
	// every relay drawn for this job, excluded by the exclusive policy's
	// retransmit draw (the other two policies ignore the ledger; as a set
	// it also collapses the independent draw's repeats). Written under mu
	// at registration; the retransmit callback is the only later writer
	relays map[peer.ID]struct{}
	// the branch proxies of every attempt of this job; mergeAnswers sorts
	// them to the end of the provider list
	proxies map[peer.ID]struct{}
	// ledger of the current attempt's SURB IDs; the retry commit clears
	// the first attempt's share, scrub deletes what remains when the job
	// ends
	surbIDs []SURBID
	// quorum bookkeeping for the current attempt, guarded by mu: every
	// SURB maps to its branch, branches counts what was launched,
	// deadBranches what failed at send and can never answer. Each branch
	// answers at most once: an OK list lands in okLists, a Failed answer
	// bumps failedBranches. An undecodable copy is no answer, it only
	// consumes its envelope (counted per branch in invalidOf); a branch
	// whose every envelope went to garbage is exhausted, nothing of it can
	// arrive anymore, and counts toward the quorum like an answered one
	branchOf          map[SURBID]int
	invalidOf         map[int]int
	branches          int
	deadBranches      int
	answeredBranches  int
	failedBranches    int
	exhaustedBranches int
	okLists           [][]peer.AddrInfo
	// the currently armed attempt timer; the retransmit swaps it under mu.
	// timerGen ties each armed timer to its attempt: a callback whose
	// generation no longer matches fired for a replaced attempt and is
	// ignored
	timer    *time.Timer
	timerGen uint64
	result   chan JobResult
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
	if cfg.InitiatorEdgeEpsilon > 1 {
		return nil, errors.New("initiator edge epsilon must be at most 1 (negative disables the bias)")
	}
	if cfg.Draw != DrawExclusive && cfg.Draw != DrawPerAttempt && cfg.Draw != DrawIndependent {
		return nil, errors.New("draw policy must be DrawExclusive, DrawPerAttempt or DrawIndependent")
	}
	if cfg.Draw == DrawIndependent && cfg.PeerState != nil && cfg.InitiatorEdgeEpsilon >= 0 {
		return nil, errors.New("initiator edge bias cannot run on the independent draw: its permutation could join repeated peers into adjacent slots (set the epsilon negative)")
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
		draw:         cfg.Draw,
		epsilon:      cfg.InitiatorEdgeEpsilon,
		state:        cfg.PeerState,
		serving:      cfg.ServingCheck,
		obs:          cfg.Observer,
		rand:         randIntn,
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
// and is closed. Every branch gets until the attempt timer to answer; the
// job settles early once all have (merged via mergeAnswers) and at the
// timer on whatever arrived. A first attempt without one OK answer, its
// branches Failed, dead at send, or silent into the timer, triggers one
// retransmit over fresh branches. ctx bounds only the first attempt's
// sends
func (jm *JobManager) StartJob(ctx context.Context, c cid.Cid) (<-chan JobResult, error) {
	// a closed manager is the terminal state, reported before the serving
	// gate: Close also clears serving, and callers expect ErrManagerClosed
	// here. The registration below rechecks under the lock for the race
	jm.mu.Lock()
	closed := jm.closed
	jm.mu.Unlock()
	if closed {
		return nil, ErrManagerClosed
	}
	// only relay-set members initiate; a client-mode start would mark this
	// node as the initiator to its first hop
	if jm.serving != nil && !jm.serving() {
		return nil, ErrClientMode
	}
	need := jm.sampleSize()
	draw := jm.drawAttempt(c, nil)
	if len(draw) < need {
		return nil, jm.poolShortfall(len(draw))
	}
	branches, err := jm.buildBranches(c, draw)
	if err != nil {
		return nil, err
	}

	// register and arm the timer before sending: a reply can arrive while
	// SendPacket still blocks
	j := &discoveryJob{
		cid:       c,
		attempt:   1,
		relays:    make(map[peer.ID]struct{}, 2*need),
		proxies:   make(map[peer.ID]struct{}, 2*jm.k),
		branchOf:  make(map[SURBID]int, 2*jm.k*jm.m),
		invalidOf: make(map[int]int, jm.k),
		result:    make(chan JobResult, 1),
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

	jm.observeAttempt(j.id, c, 1, draw)
	jm.metrics.JobsStarted.Add(1)
	sent := 0
	var dead []int
	for bi, b := range branches {
		if jm.sendBranch(ctx, b) {
			sent++
		} else {
			dead = append(dead, bi)
		}
	}
	for _, bi := range dead {
		jm.observeBranch(j.id, 1, bi, BranchDeadSend)
	}
	// dead branches leave the quorum; a first attempt with every branch
	// dead retries through the same path as one whose branches all
	// answered Failed
	if len(dead) > 0 {
		jm.markBranchesDead(j, len(dead))
	}
	return j.result, nil
}

// markBranchesDead removes n branches from the quorum: their packets never
// left, no answer can come. A reply that raced in during the sends may
// have been waiting for exactly these branches, so the quorum is
// re-checked here
func (jm *JobManager) markBranchesDead(j *discoveryJob, n int) {
	jm.mu.Lock()
	if _, ok := jm.jobs[j.id]; !ok {
		jm.mu.Unlock()
		return
	}
	j.deadBranches += n
	outcome, res := jm.quorumLocked(j)
	jm.mu.Unlock()
	switch outcome {
	case quorumSettled:
		jm.settleQuorum(j, res)
	case quorumRetry:
		// off the caller's goroutine: the retransmit draws, builds and sends
		go jm.retransmit(j)
	}
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
		jm.metrics.JobsCanceled.Add(1)
		jm.settle(j, JobResult{Err: ErrManagerClosed})
	}
}

func (jm *JobManager) poolShortfall(live int) error {
	return fmt.Errorf("%w: need %d distinct relays, pool has %d live", ErrPoolTooSmall, jm.sampleSize(), live)
}

// settle delivers res on a job already removed from the maps; the observer
// hears about the settle before the caller can see the result
func (jm *JobManager) settle(j *discoveryJob, res JobResult) {
	if jm.obs != nil {
		jm.obs.OnSettled(j.id, j.cid, settleKindOf(res), time.Now())
	}
	jm.scrub(j)
	j.result <- res
	close(j.result)
}

// buildBranches splits draw into k groups and builds one branch per group.
// Under the exclusive draw, sampling without replacement already gives the
// path invariants: no proxy on its own return paths, disjoint return
// paths, first forward relay != last return relay. The independent draw
// gives these invariants up deliberately and keeps only its own two rules
// (distinct proxies, no self-dial); see drawIndependent
func (jm *JobManager) buildBranches(c cid.Cid, draw []KeyInfo) ([]branch, error) {
	jm.biasEdgeSlots(draw)
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

// drawAttempt draws the k·(NrHops+2m) relays of one attempt. A short draw
// means pool shortfall; the caller decides. DrawIndependent dispatches to
// drawIndependent, which ignores exclude by design (its retransmit
// redraws uniformly). The rest describes the exclusive draw, minus
// exclude: minCPL = 0 is one uniform sample. Otherwise only the proxy
// slots are biased (selectProxies); the other relays are a fresh uniform
// draw around them, keeping the attempt disjoint and the relay positions
// unbiased
func (jm *JobManager) drawAttempt(c cid.Cid, exclude map[peer.ID]struct{}) []KeyInfo {
	if jm.draw == DrawIndependent {
		return jm.drawIndependent(c)
	}
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

// drawIndependent draws one attempt for the DrawIndependent policy: every
// slot an independent uniform pick from the live snapshot, with
// replacement, so no slot constrains any other beyond two hard rules. The
// k proxies stay pairwise distinct (ℓ = 0 rejects duplicate picks, which
// is uniform over distinct k-tuples; ℓ > 0 reuses selectProxies, distinct
// by construction), and no drawn node is asked to dial itself: each
// forward slot differs from its predecessor and the last relay from its
// proxy, each return path's first relay differs from the proxy that dials
// it and its second from the first. A constrained slot redraws until
// legal, which keeps it uniform over the pool minus its at most two
// forbidden peers; the snapshot holds at least the attempt's demand, so
// the redraws terminate. Admission matches the exclusive draw: fewer live
// entries than sampleSize() report a shortfall even though independent
// picks could stretch a smaller pool, keeping both policies' runs under
// identical admission
func (jm *JobManager) drawIndependent(c cid.Cid) []KeyInfo {
	need := jm.sampleSize()
	live := jm.pool.SampleExcluding(jm.pool.Len(), nil)
	if len(live) < need {
		return live
	}

	pick := func(forbidden ...peer.ID) KeyInfo {
	redraw:
		for {
			ki := live[jm.rand(len(live))]
			for _, pid := range forbidden {
				if ki.PeerID == pid {
					continue redraw
				}
			}
			return ki
		}
	}

	var proxies []KeyInfo
	if jm.minCPL > 0 {
		proxies = selectProxies(live, jm.k, jm.minCPL, providerKeyID(c))
	} else {
		seen := make(map[peer.ID]struct{}, jm.k)
		for len(proxies) < jm.k {
			ki := live[jm.rand(len(live))]
			if _, dup := seen[ki.PeerID]; dup {
				continue
			}
			seen[ki.PeerID] = struct{}{}
			proxies = append(proxies, ki)
		}
	}

	draw := make([]KeyInfo, 0, need)
	for bi := range jm.k {
		proxy := proxies[bi]
		// the forward chain up to the proxy; prev starts empty because the
		// initiator is never in the pool
		var prev peer.ID
		for i := range NrHops - 1 {
			var ki KeyInfo
			if i == NrHops-2 {
				ki = pick(prev, proxy.PeerID)
			} else {
				ki = pick(prev)
			}
			draw = append(draw, ki)
			prev = ki.PeerID
		}
		draw = append(draw, proxy)
		for range jm.m {
			s1 := pick(proxy.PeerID)
			s2 := pick(s1.PeerID)
			draw = append(draw, s1, s2)
		}
	}
	return draw
}

// mergeAnswers folds the collected branch lists into one result: the
// union of all providers, ranked by how many branches named them
// (independent confirmations first, which is what blunts a single lying
// proxy), ties in arrival order. A peer named twice by one branch counts
// once; addresses are the deduplicated union. A proxy of this job sorts
// behind every other provider whatever its confirmations, since retrieving
// from it lets it link the discovery it ran to the requester. Demoted
// rather than dropped: a proxy can be the only provider of the CID
func mergeAnswers(lists [][]peer.AddrInfo, proxies map[peer.ID]struct{}) []peer.AddrInfo {
	type slot struct {
		ai        peer.AddrInfo
		count     int
		isProxy   bool
		seenAddrs map[string]struct{}
	}
	var order []*slot
	byID := make(map[peer.ID]*slot)
	for _, list := range lists {
		counted := make(map[peer.ID]struct{}, len(list))
		for _, p := range list {
			s, ok := byID[p.ID]
			if !ok {
				_, isProxy := proxies[p.ID]
				s = &slot{ai: peer.AddrInfo{ID: p.ID}, isProxy: isProxy, seenAddrs: make(map[string]struct{})}
				byID[p.ID] = s
				order = append(order, s)
			}
			if _, dup := counted[p.ID]; !dup {
				counted[p.ID] = struct{}{}
				s.count++
			}
			for _, a := range p.Addrs {
				key := string(a.Bytes())
				if _, dup := s.seenAddrs[key]; dup {
					continue
				}
				s.seenAddrs[key] = struct{}{}
				s.ai.Addrs = append(s.ai.Addrs, a)
			}
		}
	}
	sort.SliceStable(order, func(a, b int) bool {
		if order[a].isProxy != order[b].isProxy {
			return !order[a].isProxy
		}
		return order[a].count > order[b].count
	})
	out := make([]peer.AddrInfo, len(order))
	for i, s := range order {
		out[i] = s.ai
	}
	return out
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
	type candidate struct {
		ki   KeyInfo
		dist kb.ID
	}
	var rest []candidate
	for _, ki := range live {
		id := kb.ConvertPeerID(ki.PeerID)
		if len(chosen) < k && kb.CommonPrefixLen(id, target) >= minCPL {
			chosen = append(chosen, ki)
			continue
		}
		rest = append(rest, candidate{ki, kb.Xor(id, target)})
	}
	if fill := k - len(chosen); fill > 0 {
		sort.Slice(rest, func(a, b int) bool {
			return bytes.Compare(rest[a].dist, rest[b].dist) < 0
		})
		for _, c := range rest[:fill] {
			chosen = append(chosen, c.ki)
		}
	}
	return chosen
}

// biasEdgeSlots normalizes the connection state of every relay slot whose
// observed neighbor is the initiator: the forward first hop and the last
// relay of each return path. It reads each candidate's connection state
// once here; the dial later neither rechecks nor repairs the choice, and a
// connection appearing in between is accepted. It permutes only the drawn
// relay peers across non-proxy relay slots, so the attempt-wide draw keeps
// its pairwise disjointness and the proxy slots keep their CPL-biased
// peers. A sensitive slot takes an epsilon-weighted coin over connection
// state while both classes remain available, else whatever is left (a job
// never fails on connection state); connected peers land on the
// non-sensitive slots. That parking is harmless because a non-sensitive
// slot's observable is its connection to its predecessor, another relay,
// not to the initiator. Diagnostics are recorded even with the bias
// disabled (epsilon < 0) so control runs report the natural connected
// fraction
func (jm *JobManager) biasEdgeSlots(draw []KeyInfo) {
	if jm.state == nil {
		return
	}
	g := jm.branchGroupSize()
	var r1Slots, surbLastSlots, nonSensitive []int
	for bi := range jm.k {
		base := bi * g
		r1Slots = append(r1Slots, base)
		for j := 1; j < NrHops-1; j++ {
			nonSensitive = append(nonSensitive, base+j)
		}
		for i := range jm.m {
			nonSensitive = append(nonSensitive, base+NrHops+2*i)
			surbLastSlots = append(surbLastSlots, base+NrHops+2*i+1)
		}
	}
	sensitive := append(append([]int{}, r1Slots...), surbLastSlots...)

	if jm.epsilon < 0 {
		jm.recordEdgeDiagnostics(draw, r1Slots, surbLastSlots)
		return
	}

	var connPool, freePool []KeyInfo
	for _, idx := range append(append([]int{}, sensitive...), nonSensitive...) {
		if jm.state.Connectedness(draw[idx].PeerID) == network.Connected {
			connPool = append(connPool, draw[idx])
		} else {
			freePool = append(freePool, draw[idx])
		}
	}
	jm.shuffle(connPool)
	jm.shuffle(freePool)

	// a fair processing order keeps neither R1 nor the SURB-last slots
	// systematically favored when one class runs short
	order := append([]int{}, sensitive...)
	jm.shuffleInts(order)

	assigned := make(map[int]KeyInfo, len(sensitive)+len(nonSensitive))
	for _, idx := range order {
		var wantConnected bool
		if len(connPool) > 0 && len(freePool) > 0 {
			wantConnected = jm.rand(1<<30) < int(jm.epsilon*float64(1<<30))
		} else {
			wantConnected = len(connPool) > 0
		}
		if wantConnected {
			assigned[idx] = connPool[len(connPool)-1]
			connPool = connPool[:len(connPool)-1]
		} else {
			assigned[idx] = freePool[len(freePool)-1]
			freePool = freePool[:len(freePool)-1]
		}
	}
	rest := append(connPool, freePool...)
	for i, idx := range nonSensitive {
		assigned[idx] = rest[i]
	}
	for idx, ki := range assigned {
		draw[idx] = ki
	}
	jm.recordEdgeDiagnostics(draw, r1Slots, surbLastSlots)
}

// recordEdgeDiagnostics counts the final connection state of the sensitive
// slots at selection time, split by class; the address counters are the
// c_I diagnostic on the first hop only and never steer selection
func (jm *JobManager) recordEdgeDiagnostics(draw []KeyInfo, r1Slots, surbLastSlots []int) {
	for _, idx := range r1Slots {
		if jm.state.Connectedness(draw[idx].PeerID) == network.Connected {
			jm.metrics.FirstHopConnected.Add(1)
		} else {
			jm.metrics.FirstHopUnconnected.Add(1)
		}
		if len(jm.state.Addrs(draw[idx].PeerID)) > 0 {
			jm.metrics.FirstHopWithAddrs.Add(1)
		} else {
			jm.metrics.FirstHopWithoutAddrs.Add(1)
		}
	}
	for _, idx := range surbLastSlots {
		if jm.state.Connectedness(draw[idx].PeerID) == network.Connected {
			jm.metrics.SurbLastConnected.Add(1)
		} else {
			jm.metrics.SurbLastUnconnected.Add(1)
		}
	}
}

// shuffle and shuffleInts are Fisher-Yates over the existing crypto/rand
// seam, uniform and without ordering bias
func (jm *JobManager) shuffle(s []KeyInfo) {
	for i := len(s) - 1; i > 0; i-- {
		j := jm.rand(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func (jm *JobManager) shuffleInts(s []int) {
	for i := len(s) - 1; i > 0; i-- {
		j := jm.rand(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

// registerAttemptLocked adds one attempt: SURB ledger, reply map, store
// keys, relay and proxy set, timer. Map registration and store Put must
// share the critical section (lock order mu -> SURBStore.mu): a reply can
// settle the job at any instant, and a Put after the settling scrub would
// leak
func (jm *JobManager) registerAttemptLocked(j *discoveryJob, draw []KeyInfo, branches []branch) {
	for bi, b := range branches {
		for i, id := range b.surbIDs {
			j.surbIDs = append(j.surbIDs, id)
			j.branchOf[id] = j.branches + bi
			jm.bySURB[id] = j.id
			jm.surbs.Put(id, b.surbKeys[i])
		}
	}
	j.branches += len(branches)
	for _, ki := range draw {
		j.relays[ki.PeerID] = struct{}{}
	}
	for _, proxy := range jm.branchProxies(draw) {
		j.proxies[proxy.PeerID] = struct{}{}
	}
	jm.recordProxyCPLs(j.cid, draw)
	jm.armTimerLocked(j)
}

// armTimerLocked arms the attempt timer under mu; the captured generation
// lets onTimer recognize a callback whose attempt has been replaced
func (jm *JobManager) armTimerLocked(j *discoveryJob) {
	j.timerGen++
	gen := j.timerGen
	j.timer = time.AfterFunc(jm.timeout, func() { jm.onTimer(j.id, gen) })
}

// branchProxies picks the exit slot out of every branch group of a
// full-length attempt draw
func (jm *JobManager) branchProxies(draw []KeyInfo) []KeyInfo {
	g := jm.branchGroupSize()
	proxies := make([]KeyInfo, jm.k)
	for bi := range jm.k {
		proxies[bi] = draw[bi*g+NrHops-1]
	}
	return proxies
}

// recordProxyCPLs: per branch proxy the CPL to the CID's provider key,
// recorded at every attempt (ℓ = 0 included, as baseline)
func (jm *JobManager) recordProxyCPLs(c cid.Cid, draw []KeyInfo) {
	target := providerKeyID(c)
	for _, proxy := range jm.branchProxies(draw) {
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

// answer kinds a branch can contribute; an undecodable payload is no
// answer at all (consumeInvalid)
type answerKind int

const (
	answerOK answerKind = iota
	answerFailed
)

// HandleSURBReply is the single arbitration site. Every branch gets one
// answer; the job settles as soon as all launched branches have answered
// (or died at send, or exhausted their envelopes), merging the OK lists
// via mergeAnswers. A branch that stays silent keeps the job open until
// its attempt timer, which then settles on the answers at hand or, with
// none, fires the retransmit
func (jm *JobManager) HandleSURBReply(id SURBID, payload []byte) {
	reply, err := DecodeReply(payload)
	if err != nil {
		log.Debugw("discarding undecodable surb reply", "surb", fmt.Sprintf("%x", id), "error", err)
		jm.consumeInvalid(id)
		return
	}
	if reply.Status == ReplyStatusFailed {
		if jm.answerBranch(id, answerFailed, nil) {
			jm.metrics.RepliesFailed.Add(1)
		}
		return
	}
	if jm.answerBranch(id, answerOK, reply.Providers) {
		jm.metrics.RepliesCollected.Add(1)
	}
}

// consumeInvalid retires the envelope an undecodable reply arrived
// through. The payload is authenticated garbage, so it can only come from
// the branch's own proxy; still it does not settle the branch: the
// remaining envelopes stay registered and a valid copy may yet answer.
// Only when the last envelope goes to garbage is the branch exhausted,
// and by envelope accounting rather than by trusting the garbage: no copy
// can arrive anymore, so it joins the quorum like an answered branch
func (jm *JobManager) consumeInvalid(id SURBID) {
	jm.mu.Lock()
	jobID, ok := jm.bySURB[id]
	if !ok {
		jm.mu.Unlock()
		jm.metrics.RepliesDuplicate.Add(1)
		log.Debugw("ignoring surb reply without a pending branch", "surb", fmt.Sprintf("%x", id))
		return
	}
	j := jm.jobs[jobID]
	delete(jm.bySURB, id)
	bi := j.branchOf[id]
	attempt := j.attempt
	j.invalidOf[bi]++
	exhausted := j.invalidOf[bi] == jm.m
	var outcome quorumOutcome
	var res JobResult
	if exhausted {
		j.exhaustedBranches++
		outcome, res = jm.quorumLocked(j)
	}
	jm.mu.Unlock()
	if exhausted {
		jm.observeBranch(jobID, attempt, bi, BranchExhausted)
	}
	jm.surbs.Delete(id)
	jm.metrics.RepliesInvalid.Add(1)
	switch outcome {
	case quorumSettled:
		jm.settleQuorum(j, res)
	case quorumRetry:
		// off the delivery goroutine: the retransmit draws, builds and sends
		go jm.retransmit(j)
	}
}

// answerBranch records one branch's answer and reports whether it counted
// (false = the job is gone or the branch already answered; a duplicate).
// The whole branch retires with its answer: all its SURB mappings and
// store entries go, so the proxy's identical copies through the other
// envelopes die as duplicates while the job keeps waiting on the
// remaining branches
func (jm *JobManager) answerBranch(id SURBID, kind answerKind, providers []peer.AddrInfo) bool {
	jm.mu.Lock()
	jobID, ok := jm.bySURB[id]
	if !ok {
		jm.mu.Unlock()
		jm.metrics.RepliesDuplicate.Add(1)
		log.Debugw("ignoring surb reply without a pending branch", "surb", fmt.Sprintf("%x", id))
		return false
	}
	j := jm.jobs[jobID]
	bi := j.branchOf[id]
	attempt := j.attempt
	stale := make([]SURBID, 0, jm.m)
	for _, sid := range j.surbIDs {
		if j.branchOf[sid] == bi {
			delete(jm.bySURB, sid)
			stale = append(stale, sid)
		}
	}
	j.answeredBranches++
	switch kind {
	case answerOK:
		j.okLists = append(j.okLists, providers)
	case answerFailed:
		j.failedBranches++
	}
	outcome, res := jm.quorumLocked(j)
	jm.mu.Unlock()

	switch kind {
	case answerOK:
		jm.observeBranch(jobID, attempt, bi, BranchOK)
	case answerFailed:
		jm.observeBranch(jobID, attempt, bi, BranchFailed)
	}

	// idempotent against a concurrent settle's scrub
	for _, sid := range stale {
		jm.surbs.Delete(sid)
	}
	switch outcome {
	case quorumSettled:
		jm.settleQuorum(j, res)
	case quorumRetry:
		// off the delivery goroutine: the retransmit draws, builds and sends
		go jm.retransmit(j)
	}
	return true
}

// quorum outcomes: the job stays open, settles on the collected answers,
// or has spent its first attempt without one OK answer and retries
type quorumOutcome int

const (
	quorumOpen quorumOutcome = iota
	quorumSettled
	quorumRetry
)

// quorumLocked checks whether every launched branch has answered, died or
// exhausted its envelopes. With an OK answer at hand, or no retry left,
// it removes the job from the maps and builds the result; a first attempt
// whose branches all answered Failed, died or exhausted commits the retry
// instead: reported failure retries like silence, and with every branch
// accounted for there is nothing left to wait for. Caller holds mu; on
// quorumRetry it follows up with retransmit off the lock
func (jm *JobManager) quorumLocked(j *discoveryJob) (quorumOutcome, JobResult) {
	if j.branches == 0 || j.answeredBranches+j.deadBranches+j.exhaustedBranches < j.branches {
		return quorumOpen, JobResult{}
	}
	if len(j.okLists) == 0 && j.attempt == 1 && !jm.noRetransmit {
		jm.commitRetryLocked(j)
		return quorumRetry, JobResult{}
	}
	jm.unregisterLocked(j)
	return quorumSettled, resultOf(j)
}

// commitRetryLocked switches j to its second and final attempt and
// invalidates the first attempt's timer; the bumped generation covers a
// timer that has already fired and is waiting on mu. The first attempt
// ends here, so its branch groups leave the SURB table and its quorum
// counters reset: a reply the retry overtook is dropped as a duplicate,
// and the retry settles on its own k branches. The relay and proxy
// ledgers survive, the retry's draw and the merge demotion need the whole
// job's history. Caller holds mu
func (jm *JobManager) commitRetryLocked(j *discoveryJob) {
	j.attempt = 2
	j.timerGen++
	if j.timer != nil {
		j.timer.Stop()
	}
	for _, sid := range j.surbIDs {
		delete(jm.bySURB, sid)
		jm.surbs.Delete(sid)
		delete(j.branchOf, sid)
	}
	j.surbIDs = j.surbIDs[:0]
	clear(j.invalidOf)
	j.branches, j.deadBranches, j.answeredBranches, j.failedBranches, j.exhaustedBranches = 0, 0, 0, 0, 0
	j.okLists = nil
}

// resultOf: any OK answer makes the job a success on the merged lists;
// otherwise a Failed answer means a proxy reported a discovery failure,
// exhausted branches delivered only garbage, and without either every
// launched branch died at send
func resultOf(j *discoveryJob) JobResult {
	if len(j.okLists) > 0 {
		return JobResult{Providers: mergeAnswers(j.okLists, j.proxies)}
	}
	if j.failedBranches > 0 {
		return JobResult{Err: ErrDiscoveryFailed}
	}
	if j.exhaustedBranches > 0 {
		return JobResult{Err: ErrInvalidReplies}
	}
	return JobResult{Err: ErrSendFailed}
}

// settleKindOf labels a job result for the observer; the errors.Is chain
// also catches the wrapped retransmit shortfall
func settleKindOf(res JobResult) SettleKind {
	switch {
	case res.Err == nil:
		return SettleSuccess
	case errors.Is(res.Err, ErrJobTimeout):
		return SettleTimeout
	case errors.Is(res.Err, ErrDiscoveryFailed):
		return SettleFailed
	case errors.Is(res.Err, ErrInvalidReplies):
		return SettleInvalidReplies
	case errors.Is(res.Err, ErrSendFailed):
		return SettleSendFailed
	case errors.Is(res.Err, ErrPoolTooSmall):
		return SettlePoolShortfall
	case errors.Is(res.Err, ErrManagerClosed):
		return SettleClosed
	}
	return SettleOther
}

// settleQuorum settles a job already removed from the maps and books the
// outcome
func (jm *JobManager) settleQuorum(j *discoveryJob, res JobResult) {
	if res.Err != nil {
		jm.metrics.JobsFailed.Add(1)
	} else {
		jm.metrics.JobsSucceeded.Add(1)
	}
	jm.settle(j, res)
}

// onTimer ends the waiting for silent branches: with at least one OK
// answer at hand the job settles on it (a lone answer is a result, only
// the chance for the others ends), with none the first timer commits the
// retransmit and the second settles as ErrJobTimeout. A stale generation
// means the attempt was replaced while this callback fired; the new
// attempt runs under its own timer
func (jm *JobManager) onTimer(jobID uint64, gen uint64) {
	jm.mu.Lock()
	j, ok := jm.jobs[jobID]
	if !ok || gen != j.timerGen {
		jm.mu.Unlock()
		return
	}
	if len(j.okLists) > 0 {
		jm.unregisterLocked(j)
		res := resultOf(j)
		jm.mu.Unlock()
		jm.settleQuorum(j, res)
		return
	}
	if j.attempt == 1 && !jm.noRetransmit {
		jm.commitRetryLocked(j)
		jm.mu.Unlock()
		jm.retransmit(j)
		return
	}
	jm.unregisterLocked(j)
	jm.mu.Unlock()
	jm.metrics.JobsTimedOut.Add(1)
	jm.settle(j, JobResult{Err: ErrJobTimeout})
}

// retransmit runs the second and final attempt after the caller committed
// it under mu: fresh draw (avoiding the first attempt's relays when the
// pool allows), k new branches, one final timer. If even the unrestricted
// draw is short there is no second wave and the job settles on the
// shortfall: the commit already ended the first attempt, so no reply can
// arrive anymore
func (jm *JobManager) retransmit(j *discoveryJob) {
	need := jm.sampleSize()
	// only the exclusive draw avoids the first attempt's relays, with the
	// unrestricted fallback when the exclusion would starve the pool. The
	// other two policies redraw without it and need no fallback:
	// DrawPerAttempt takes a fresh exclusive sample, DrawIndependent
	// repeats its per-slot draw
	var exclude map[peer.ID]struct{}
	if jm.draw == DrawExclusive {
		exclude = j.relays
	}
	draw := jm.drawAttempt(j.cid, exclude)
	if len(draw) < need && exclude != nil {
		draw = jm.drawAttempt(j.cid, nil)
	}
	var branches []branch
	var err error
	if len(draw) < need {
		err = jm.poolShortfall(len(draw))
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
	if err != nil {
		// the commit already dropped the first attempt's SURBs, so nothing
		// can arrive anymore; the job settles on the error at once
		jm.unregisterLocked(j)
		jm.mu.Unlock()
		jm.metrics.RetransmitsFailed.Add(1)
		log.Debugw("retransmit impossible, settling the job", "cid", j.cid, "error", err)
		jm.settleQuorum(j, JobResult{Err: fmt.Errorf("retransmit: %w", err)})
		return
	}
	jm.registerAttemptLocked(j, draw, branches)
	jm.mu.Unlock()

	jm.observeAttempt(j.id, j.cid, 2, draw)
	jm.metrics.Retransmits.Add(1)

	// the StartJob ctx bounded only the first attempt; even if every send
	// fails, the final timer settles the job
	sent := 0
	var dead []int
	for bi, b := range branches {
		sctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
		if jm.sendBranch(sctx, b) {
			sent++
		} else {
			dead = append(dead, bi)
		}
		cancel()
	}
	for _, bi := range dead {
		jm.observeBranch(j.id, 2, bi, BranchDeadSend)
	}
	if len(dead) > 0 {
		jm.markBranchesDead(j, len(dead))
	}
}

func (jm *JobManager) unregisterLocked(j *discoveryJob) {
	delete(jm.jobs, j.id)
	for _, sid := range j.surbIDs {
		delete(jm.bySURB, sid)
	}
}

// scrub stops the timer and deletes the job's whole SURB ledger, used or
// not. Callers own j exclusively (claimed via quorumLocked/Close), so no
// lock
func (jm *JobManager) scrub(j *discoveryJob) {
	if j.timer != nil {
		j.timer.Stop()
	}
	for _, sid := range j.surbIDs {
		jm.surbs.Delete(sid)
	}
}

// attemptPaths reads the final slot assignment out of a full attempt draw;
// call it only after buildBranches, whose edge bias permutes the draw
func (jm *JobManager) attemptPaths(draw []KeyInfo) []BranchPath {
	g := jm.branchGroupSize()
	paths := make([]BranchPath, jm.k)
	for bi := range paths {
		group := draw[bi*g : (bi+1)*g]
		fwd := make([]peer.ID, NrHops)
		for i, ki := range group[:NrHops] {
			fwd[i] = ki.PeerID
		}
		rets := make([][]peer.ID, jm.m)
		for i := range jm.m {
			rets[i] = []peer.ID{group[NrHops+2*i].PeerID, group[NrHops+2*i+1].PeerID}
		}
		paths[bi] = BranchPath{Forward: fwd, Returns: rets}
	}
	return paths
}

func (jm *JobManager) observeAttempt(jobID uint64, c cid.Cid, attempt int, draw []KeyInfo) {
	if jm.obs == nil {
		return
	}
	jm.obs.OnAttempt(jobID, c, attempt, jm.attemptPaths(draw))
}

func (jm *JobManager) observeBranch(jobID uint64, attempt, branch int, o BranchOutcome) {
	if jm.obs == nil {
		return
	}
	jm.obs.OnBranchOutcome(jobID, attempt, branch, o, time.Now())
}
