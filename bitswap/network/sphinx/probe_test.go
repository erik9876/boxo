package sphinx

import (
	"context"
	"sync"
	"testing"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type sentMsg struct {
	to  peer.ID
	msg bsmsg.BitSwapMessage
}

type fakeWantSender struct {
	mu   sync.Mutex
	sent []sentMsg
}

func (s *fakeWantSender) SendMessage(_ context.Context, p peer.ID, m bsmsg.BitSwapMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, sentMsg{to: p, msg: m})
	return nil
}

func (s *fakeWantSender) snapshot() []sentMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentMsg(nil), s.sent...)
}

type fakeNeighbors struct {
	peers []peer.ID
	addrs map[peer.ID][]ma.Multiaddr
}

func (f fakeNeighbors) Peers() []peer.ID                { return f.peers }
func (f fakeNeighbors) Addrs(p peer.ID) []ma.Multiaddr { return f.addrs[p] }

func probePeers(t *testing.T, n int) []peer.ID {
	t.Helper()
	ps := make([]peer.ID, n)
	for i := range ps {
		_, ps[i] = newIdentity(t)
	}
	return ps
}

// waitUntil polls cond until it holds or the deadline passes
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// haveMsg is a neighbor's reply announcing it holds c
func haveMsg(c cid.Cid) bsmsg.BitSwapMessage {
	m := bsmsg.New(false)
	m.AddHave(c)
	return m
}

// every neighbor gets exactly one broadcast message carrying a single
// non-cancel WANT-HAVE without SendDontHave, the vanilla broadcast shape
func TestProbeBroadcastsWantHaveToAllNeighbors(t *testing.T) {
	c := testCID(t)
	peers := probePeers(t, 3)
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{peers: peers}, ProbeConfig{Window: 20 * time.Millisecond})

	got := p.Probe(context.Background(), c, 16)
	if len(got) != 0 {
		t.Fatalf("probe without replies returned %d providers", len(got))
	}

	waitUntil(t, time.Second, func() bool { return len(sender.snapshot()) >= 3 }, "broadcast never reached all neighbors")
	seen := make(map[peer.ID]bool)
	for _, sm := range sender.snapshot() {
		wl := sm.msg.Wantlist()
		if len(wl) != 1 {
			continue // cancel round or unrelated
		}
		e := wl[0]
		if e.Cancel {
			continue
		}
		if !e.Cid.Equals(c) || e.WantType != pb.Message_Wantlist_Have || e.SendDontHave {
			t.Fatalf("malformed probe entry: %+v", e)
		}
		seen[sm.to] = true
	}
	for _, pid := range peers {
		if !seen[pid] {
			t.Fatalf("neighbor %s never got the want-have", pid)
		}
	}
}

// HAVE replies within the window become providers, with peerstore addresses
func TestProbeCollectsHaveResponders(t *testing.T) {
	c := testCID(t)
	peers := probePeers(t, 3)
	addr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	nb := fakeNeighbors{peers: peers, addrs: map[peer.ID][]ma.Multiaddr{peers[0]: {addr}}}
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, nb, ProbeConfig{Window: 300 * time.Millisecond})

	go func() {
		time.Sleep(30 * time.Millisecond)
		p.ReceiveMessage(context.Background(), peers[0], haveMsg(c))
		p.ReceiveMessage(context.Background(), peers[1], haveMsg(c))
	}()

	got := p.Probe(context.Background(), c, 16)
	if len(got) != 2 {
		t.Fatalf("got %d providers, want 2", len(got))
	}
	byID := make(map[peer.ID][]ma.Multiaddr)
	for _, ai := range got {
		byID[ai.ID] = ai.Addrs
	}
	if _, ok := byID[peers[0]]; !ok {
		t.Fatalf("responder %s missing from providers", peers[0])
	}
	if _, ok := byID[peers[1]]; !ok {
		t.Fatalf("responder %s missing from providers", peers[1])
	}
	if len(byID[peers[0]]) != 1 || !byID[peers[0]][0].Equal(addr) {
		t.Fatalf("provider %s lost its peerstore address: %v", peers[0], byID[peers[0]])
	}
}

// the limit ends the window early; the probe must not sit out the full
// window once it cannot accept more providers
func TestProbeStopsEarlyAtLimit(t *testing.T) {
	c := testCID(t)
	peers := probePeers(t, 3)
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{peers: peers}, ProbeConfig{Window: 10 * time.Second})

	go func() {
		time.Sleep(20 * time.Millisecond)
		for _, pid := range peers {
			p.ReceiveMessage(context.Background(), pid, haveMsg(c))
		}
	}()

	start := time.Now()
	got := p.Probe(context.Background(), c, 2)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("probe waited out the window despite a full result (%s)", elapsed)
	}
	if len(got) != 2 {
		t.Fatalf("got %d providers, want limit 2", len(got))
	}
}

// after the window every probed neighbor gets a cancel for the want, like
// a vanilla client canceling a satisfied broadcast want
func TestProbeCancelsAfterWindow(t *testing.T) {
	c := testCID(t)
	peers := probePeers(t, 2)
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{peers: peers}, ProbeConfig{Window: 20 * time.Millisecond})

	p.Probe(context.Background(), c, 16)

	canceled := func() map[peer.ID]bool {
		out := make(map[peer.ID]bool)
		for _, sm := range sender.snapshot() {
			for _, e := range sm.msg.Wantlist() {
				if e.Cancel && e.Cid.Equals(c) {
					out[sm.to] = true
				}
			}
		}
		return out
	}
	waitUntil(t, time.Second, func() bool { return len(canceled()) == 2 }, "cancel never reached both neighbors")
}

// repeated HAVEs from one peer count once; HAVEs for foreign CIDs are not
// this probe's business
func TestProbeDedupesAndIgnoresUnrelatedHaves(t *testing.T) {
	c := testCID(t)
	other := identityCID(t, 36)
	peers := probePeers(t, 2)
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{peers: peers}, ProbeConfig{Window: 200 * time.Millisecond})

	go func() {
		time.Sleep(20 * time.Millisecond)
		p.ReceiveMessage(context.Background(), peers[0], haveMsg(c))
		p.ReceiveMessage(context.Background(), peers[0], haveMsg(c))
		p.ReceiveMessage(context.Background(), peers[1], haveMsg(other))
	}()

	got := p.Probe(context.Background(), c, 16)
	if len(got) != 1 || got[0].ID != peers[0] {
		t.Fatalf("got %v, want exactly one entry for %s", got, peers[0])
	}
}

// servers answer a WANT-HAVE for a small block with the block itself; the
// sender still counts as provider
func TestProbeCountsDirectBlockAsHave(t *testing.T) {
	blk := blocks.NewBlock([]byte("tiny probe block"))
	peers := probePeers(t, 1)
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{peers: peers}, ProbeConfig{Window: 200 * time.Millisecond})

	go func() {
		time.Sleep(20 * time.Millisecond)
		m := bsmsg.New(false)
		m.AddBlock(blk)
		p.ReceiveMessage(context.Background(), peers[0], m)
	}()

	got := p.Probe(context.Background(), blk.Cid(), 16)
	if len(got) != 1 || got[0].ID != peers[0] {
		t.Fatalf("block reply not counted as provider: %v", got)
	}
}

// no neighbors means nothing to ask: no result and no sends
func TestProbeWithoutNeighbors(t *testing.T) {
	sender := &fakeWantSender{}
	p := NewWantHaveProber(sender, fakeNeighbors{}, ProbeConfig{Window: 20 * time.Millisecond})

	if got := p.Probe(context.Background(), testCID(t), 16); got != nil {
		t.Fatalf("probe without neighbors returned %v", got)
	}
	if n := len(sender.snapshot()); n != 0 {
		t.Fatalf("probe without neighbors sent %d messages", n)
	}
}

type fakeProber struct {
	result []peer.AddrInfo
}

func (f fakeProber) Probe(context.Context, cid.Cid, int) []peer.AddrInfo { return f.result }

type fakeRoute struct {
	mu        sync.Mutex
	calls     int
	lastLimit int
	result    []peer.AddrInfo
	err       error
}

func (f *fakeRoute) FindProviders(_ context.Context, _ cid.Cid, limit int) ([]peer.AddrInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastLimit = limit
	return f.result, f.err
}

// a probe hit answers the query; the routing backend stays untouched
func TestProbeThenRouteSkipsRouteOnProbeHit(t *testing.T) {
	_, pid := newIdentity(t)
	hit := []peer.AddrInfo{{ID: pid}}
	route := &fakeRoute{result: []peer.AddrInfo{{ID: pid}, {ID: pid}}}
	d := ProbeThenRouteDiscoverer{Probe: fakeProber{result: hit}, Route: route}

	got, err := d.FindProviders(context.Background(), testCID(t), 16)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(got) != 1 || got[0].ID != pid {
		t.Fatalf("got %v, want the probe hit", got)
	}
	if route.calls != 0 {
		t.Fatalf("routing backend was queried %d times despite a probe hit", route.calls)
	}
}

// an empty probe falls back to the routing backend with the same limit
func TestProbeThenRouteFallsBackOnEmptyProbe(t *testing.T) {
	_, pid := newIdentity(t)
	route := &fakeRoute{result: []peer.AddrInfo{{ID: pid}}}
	d := ProbeThenRouteDiscoverer{Probe: fakeProber{}, Route: route}

	got, err := d.FindProviders(context.Background(), testCID(t), 16)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(got) != 1 || got[0].ID != pid {
		t.Fatalf("got %v, want the routed providers", got)
	}
	if route.calls != 1 || route.lastLimit != 16 {
		t.Fatalf("routing backend saw %d calls with limit %d, want 1 call with 16", route.calls, route.lastLimit)
	}
}

// a routing failure after an empty probe stays a discovery failure
func TestProbeThenRoutePropagatesRouteError(t *testing.T) {
	route := &fakeRoute{err: context.DeadlineExceeded}
	d := ProbeThenRouteDiscoverer{Probe: fakeProber{}, Route: route}

	if _, err := d.FindProviders(context.Background(), testCID(t), 16); err == nil {
		t.Fatal("routing error was swallowed")
	}
}

type probeObservation struct {
	c          cid.Cid
	hit        bool
	start      time.Time
	firstHave  time.Time
	responders int
}

func TestProbeObserverReportsFirstHave(t *testing.T) {
	c := testCID(t)
	peers := probePeers(t, 3)
	var obs []probeObservation
	var obsMu sync.Mutex
	p := NewWantHaveProber(&fakeWantSender{}, fakeNeighbors{peers: peers}, ProbeConfig{
		Window: time.Second,
		Observer: func(c cid.Cid, hit bool, start, firstHave time.Time, responders int) {
			obsMu.Lock()
			defer obsMu.Unlock()
			obs = append(obs, probeObservation{c, hit, start, firstHave, responders})
		},
	})

	before := time.Now()
	done := make(chan []peer.AddrInfo, 1)
	go func() { done <- p.Probe(context.Background(), c, 1) }()

	waitUntil(t, time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.watches[c]) == 1
	}, "probe never registered its watch")
	p.ReceiveMessage(context.Background(), peers[0], haveMsg(c))
	<-done

	obsMu.Lock()
	defer obsMu.Unlock()
	if len(obs) != 1 {
		t.Fatalf("observer called %d times, want 1", len(obs))
	}
	o := obs[0]
	if !o.hit || o.responders != 1 {
		t.Errorf("observation hit=%t responders=%d, want true/1", o.hit, o.responders)
	}
	if o.firstHave.IsZero() || o.firstHave.Before(before) || o.firstHave.Before(o.start) {
		t.Errorf("first-have time %v implausible (start %v)", o.firstHave, o.start)
	}
}

func TestProbeObserverReportsMiss(t *testing.T) {
	c := testCID(t)
	var obs []probeObservation
	var obsMu sync.Mutex
	p := NewWantHaveProber(&fakeWantSender{}, fakeNeighbors{peers: probePeers(t, 2)}, ProbeConfig{
		Window: 30 * time.Millisecond,
		Observer: func(c cid.Cid, hit bool, start, firstHave time.Time, responders int) {
			obsMu.Lock()
			defer obsMu.Unlock()
			obs = append(obs, probeObservation{c, hit, start, firstHave, responders})
		},
	})

	if got := p.Probe(context.Background(), c, 4); got != nil {
		t.Fatalf("empty probe returned %v", got)
	}
	obsMu.Lock()
	defer obsMu.Unlock()
	if len(obs) != 1 {
		t.Fatalf("observer called %d times, want 1", len(obs))
	}
	if obs[0].hit || !obs[0].firstHave.IsZero() || obs[0].responders != 0 {
		t.Errorf("miss observation = %+v, want hit=false, zero first-have, 0 responders", obs[0])
	}
}
