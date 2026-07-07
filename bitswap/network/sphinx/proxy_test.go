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

// runningJobs reports jobs currently executing; test hook
func (p *Proxy) runningJobs() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// fakeDiscoverer is a canned ProviderDiscoverer. With block set it waits
// for ctx before returning, standing in for a discovery that exhausts the
// proxy timeout
type fakeDiscoverer struct {
	providers []peer.AddrInfo
	err       error
	block     bool

	mu    sync.Mutex
	calls int
}

func (d *fakeDiscoverer) FindProviders(ctx context.Context, c cid.Cid, limit int) ([]peer.AddrInfo, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if d.block {
		<-ctx.Done()
	}
	return d.providers, d.err
}

type sentPacket struct {
	to  peer.ID
	pkt []byte
}

// fakeSender captures SendPacket calls; err, when set, fails every send
type fakeSender struct {
	mu   sync.Mutex
	sent []sentPacket
	err  error
}

func (s *fakeSender) SendPacket(_ context.Context, next peer.ID, pkt []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, sentPacket{to: next, pkt: append([]byte(nil), pkt...)})
	return nil
}

func (s *fakeSender) snapshot() []sentPacket {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentPacket(nil), s.sent...)
}

// waitFor polls cond until it holds or the deadline expires
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(d)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
			if cond() {
				return
			}
		}
	}
}

// buildTestJob encodes a job with m real SURBs and returns the payload plus
// the first return hop of each SURB: the peers a proxy must send the reply
// to
func buildTestJob(t *testing.T, m int) (payload []byte, firstHops []peer.ID) {
	t.Helper()
	surbs := make([][]byte, m)
	firstHops = make([]peer.ID, m)
	for i := range surbs {
		hops := newHopInfos(t, NrHops)
		surb, _, _, err := NewSURB(hops)
		if err != nil {
			t.Fatalf("NewSURB: %v", err)
		}
		surbs[i], firstHops[i] = surb, hops[0].PeerID
	}
	payload, err := EncodeJob(testCID(t), surbs)
	if err != nil {
		t.Fatalf("EncodeJob: %v", err)
	}
	return payload, firstHops
}

func newTestProxy(t *testing.T, sender PacketSender, cfg ProxyConfig) *Proxy {
	t.Helper()
	p, err := NewProxy(sender, cfg)
	if err != nil {
		t.Fatalf("NewProxy: %v", err)
	}
	return p
}

func TestProxyRepliesThroughAllSURBs(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{Discoverer: &fakeDiscoverer{providers: testProviders(t, 3)}})
	payload, firstHops := buildTestJob(t, ReturnPathsPerJob)

	p.HandleDelivery(DiscoveryRecipient, payload)
	waitFor(t, 5*time.Second, "all replies", func() bool { return len(sender.snapshot()) == ReturnPathsPerJob })

	packetLen := Geometry().PacketLength
	for i, s := range sender.snapshot() {
		if len(s.pkt) != packetLen {
			t.Errorf("reply %d is %d bytes, want %d", i, len(s.pkt), packetLen)
		}
		if s.to != firstHops[i] {
			t.Errorf("reply %d sent to %s, want first return hop %s", i, s.to, firstHops[i])
		}
	}
	waitFor(t, 2*time.Second, "job state to die", func() bool { return p.runningJobs() == 0 })
}

func TestProxyEmptyProviderListStillReplies(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{Discoverer: &fakeDiscoverer{}})
	payload, _ := buildTestJob(t, 2)

	p.HandleDelivery(DiscoveryRecipient, payload)
	waitFor(t, 5*time.Second, "replies for an empty result", func() bool { return len(sender.snapshot()) == 2 })
	waitFor(t, 2*time.Second, "job state to die", func() bool { return p.runningJobs() == 0 })
}

func TestProxyDiscovererTimeoutStillReplies(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{
		Discoverer: &fakeDiscoverer{block: true},
		Timeout:    100 * time.Millisecond,
	})
	payload, _ := buildTestJob(t, 2)

	p.HandleDelivery(DiscoveryRecipient, payload)
	// The blocked discoverer returns at the proxy timeout; the reply (with
	// whatever was found, here nothing) still goes out through every SURB
	waitFor(t, 5*time.Second, "replies after discoverer timeout", func() bool { return len(sender.snapshot()) == 2 })
	waitFor(t, 2*time.Second, "job state to die", func() bool { return p.runningJobs() == 0 })
}

func TestProxyDiscovererErrorStillReplies(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{
		Discoverer: &fakeDiscoverer{err: errors.New("dht exploded")},
	})
	payload, _ := buildTestJob(t, 2)

	p.HandleDelivery(DiscoveryRecipient, payload)
	// Failure is reported through the SURBs (status failed), never dropped
	waitFor(t, 5*time.Second, "failed-status replies", func() bool { return len(sender.snapshot()) == 2 })
}

func TestProxyDropsWrongRecipient(t *testing.T) {
	sender := &fakeSender{}
	disc := &fakeDiscoverer{}
	p := newTestProxy(t, sender, ProxyConfig{Discoverer: disc})
	payload, _ := buildTestJob(t, 1)

	p.HandleDelivery(RecipientID{}, payload)
	time.Sleep(100 * time.Millisecond)
	if got := len(sender.snapshot()); got != 0 {
		t.Errorf("wrong recipient produced %d sends", got)
	}
	disc.mu.Lock()
	calls := disc.calls
	disc.mu.Unlock()
	if calls != 0 {
		t.Errorf("wrong recipient reached the discoverer %d times", calls)
	}
}

func TestProxyDropsUndecodableJob(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{Discoverer: &fakeDiscoverer{}})

	p.HandleDelivery(DiscoveryRecipient, []byte("not a job"))
	time.Sleep(100 * time.Millisecond)
	if got := len(sender.snapshot()); got != 0 {
		t.Errorf("undecodable job produced %d sends", got)
	}
	if p.runningJobs() != 0 {
		t.Error("undecodable job left running state")
	}
}

func TestProxyCountsExecutedJobs(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{Discoverer: &fakeDiscoverer{providers: testProviders(t, 1)}})
	payload, _ := buildTestJob(t, 1)

	p.HandleDelivery(DiscoveryRecipient, payload)
	waitFor(t, 5*time.Second, "the job's reply", func() bool { return len(sender.snapshot()) == 1 })
	if got := p.Metrics().JobsExecuted; got != 1 {
		t.Errorf("JobsExecuted = %d, want 1", got)
	}

	// Dropped deliveries never count as executed jobs
	p.HandleDelivery(RecipientID{}, payload)
	p.HandleDelivery(DiscoveryRecipient, []byte("not a job"))
	time.Sleep(100 * time.Millisecond)
	if got := p.Metrics().JobsExecuted; got != 1 {
		t.Errorf("JobsExecuted after dropped deliveries = %d, want still 1", got)
	}
}

func TestProxyConcurrencyCapDropsExcessJobs(t *testing.T) {
	sender := &fakeSender{}
	p := newTestProxy(t, sender, ProxyConfig{
		Discoverer:        &fakeDiscoverer{block: true},
		Timeout:           200 * time.Millisecond,
		MaxConcurrentJobs: 1,
	})
	first, _ := buildTestJob(t, 1)
	second, _ := buildTestJob(t, 1)

	p.HandleDelivery(DiscoveryRecipient, first)
	waitFor(t, 2*time.Second, "first job to occupy the slot", func() bool { return p.runningJobs() == 1 })
	p.HandleDelivery(DiscoveryRecipient, second) // over the cap: dropped

	// Only the first job ever replies; the dropped one leaves no trace
	waitFor(t, 5*time.Second, "the first job's reply", func() bool { return len(sender.snapshot()) == 1 })
	waitFor(t, 2*time.Second, "job state to die", func() bool { return p.runningJobs() == 0 })
	time.Sleep(100 * time.Millisecond)
	if got := len(sender.snapshot()); got != 1 {
		t.Errorf("dropped job still produced sends: %d total", got)
	}
}
