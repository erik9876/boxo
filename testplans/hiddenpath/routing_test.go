package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// fakeRouting hands out a pre-filled provider channel; Provide is a no-op
type fakeRouting struct {
	ch chan peer.AddrInfo
}

func (f fakeRouting) Provide(context.Context, cid.Cid, bool) error {
	return nil
}

func (f fakeRouting) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return f.ch
}

type walkObservation struct {
	c           cid.Cid
	start       time.Time
	firstRecord time.Time
	end         time.Time
	providers   int
}

func TestObservedRoutingRelaysAndObserves(t *testing.T) {
	c := mustTestCID(t)
	inner := fakeRouting{ch: make(chan peer.AddrInfo, 3)}
	observed := make(chan walkObservation, 1)
	o := observedRouting{inner: inner, observer: func(c cid.Cid, start, firstRecord, end time.Time, providers int) {
		observed <- walkObservation{c, start, firstRecord, end, providers}
	}}

	before := time.Now()
	out := o.FindProvidersAsync(context.Background(), c, 16)

	// an empty-ID entry passes through untouched but is no record
	inner.ch <- peer.AddrInfo{}
	time.Sleep(50 * time.Millisecond)
	inner.ch <- peer.AddrInfo{ID: peer.ID("prov-1")}
	inner.ch <- peer.AddrInfo{ID: peer.ID("prov-2")}
	close(inner.ch)

	var got []peer.AddrInfo
	for p := range out {
		got = append(got, p)
	}
	if len(got) != 3 || got[1].ID != peer.ID("prov-1") || got[2].ID != peer.ID("prov-2") {
		t.Fatalf("relayed = %+v, want empty entry then prov-1, prov-2", got)
	}

	select {
	case w := <-observed:
		if w.c != c || w.providers != 2 {
			t.Errorf("observation = cid %s / %d providers, want %s / 2", w.c, w.providers, c)
		}
		if w.firstRecord.IsZero() || w.firstRecord.Before(before.Add(40*time.Millisecond)) {
			t.Errorf("firstRecord = %v, want stamped at the first non-empty record (>= start+40ms)", w.firstRecord)
		}
		if w.start.Before(before) || w.end.Before(w.firstRecord) {
			t.Errorf("start/end = %v/%v out of order around firstRecord %v", w.start, w.end, w.firstRecord)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer never fired after channel close")
	}
}

func TestObservedRoutingDeliversAfterCtxCancel(t *testing.T) {
	c := mustTestCID(t)
	inner := fakeRouting{ch: make(chan peer.AddrInfo, 20)}
	o := observedRouting{inner: inner}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := o.FindProvidersAsync(ctx, c, 16)
	for i := 0; i < 20; i++ {
		inner.ch <- peer.AddrInfo{ID: peer.ID(fmt.Sprintf("prov-%d", i))}
	}
	close(inner.ch)

	got := 0
	for range out {
		got++
	}
	if got != 20 {
		t.Fatalf("delivered %d of 20 records under an expired ctx; the consumer drains to close, the relay must not drop", got)
	}
}

func TestObservedRoutingClosesBeforeObserver(t *testing.T) {
	c := mustTestCID(t)
	inner := fakeRouting{ch: make(chan peer.AddrInfo, 1)}
	closedFirst := make(chan bool, 1)
	var out <-chan peer.AddrInfo
	o := observedRouting{inner: inner, observer: func(cid.Cid, time.Time, time.Time, time.Time, int) {
		select {
		case _, open := <-out:
			closedFirst <- !open
		default:
			closedFirst <- false
		}
	}}

	out = o.FindProvidersAsync(context.Background(), c, 16)
	inner.ch <- peer.AddrInfo{ID: peer.ID("prov-1")}
	close(inner.ch)
	if p, open := <-out; !open || p.ID != peer.ID("prov-1") {
		t.Fatalf("relay = %v open=%t, want prov-1", p, open)
	}

	select {
	case ok := <-closedFirst:
		if !ok {
			t.Fatal("observer ran before the channel closed; the consumer then waits on the event-log mutex inside the measured path")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer never fired")
	}
}

func TestObservedRoutingEmptyWalk(t *testing.T) {
	c := mustTestCID(t)
	inner := fakeRouting{ch: make(chan peer.AddrInfo)}
	observed := make(chan walkObservation, 1)
	o := observedRouting{inner: inner, observer: func(c cid.Cid, start, firstRecord, end time.Time, providers int) {
		observed <- walkObservation{c, start, firstRecord, end, providers}
	}}

	out := o.FindProvidersAsync(context.Background(), c, 16)
	close(inner.ch)
	for range out {
	}

	select {
	case w := <-observed:
		if w.providers != 0 || !w.firstRecord.IsZero() {
			t.Errorf("observation = %d providers / firstRecord %v, want 0 / zero time", w.providers, w.firstRecord)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer never fired for the empty walk")
	}
}
