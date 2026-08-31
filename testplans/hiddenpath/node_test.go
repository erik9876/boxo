package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type staticDiscoverer struct {
	provs []peer.AddrInfo
	err   error
}

func (d staticDiscoverer) FindProviders(_ context.Context, _ cid.Cid, limit int) ([]peer.AddrInfo, error) {
	if d.err != nil {
		return nil, d.err
	}
	if limit < len(d.provs) {
		return d.provs[:limit], d.err
	}
	return d.provs, d.err
}

func TestDiscovererFinderStreamsAndCloses(t *testing.T) {
	want := []peer.AddrInfo{{ID: peer.ID("p1")}, {ID: peer.ID("p2")}}
	f := discovererFinder{disc: staticDiscoverer{provs: want}}

	got := make([]peer.AddrInfo, 0, 2)
	for p := range f.FindProvidersAsync(context.Background(), mustTestCID(t), 0) {
		got = append(got, p)
	}
	if len(got) != 2 || got[0].ID != want[0].ID || got[1].ID != want[1].ID {
		t.Fatalf("streamed %v, want %v in order", got, want)
	}
}

// wallDiscoverer models the churn dial wall: the walk only returns when
// its ctx dies, with the records it had found long before
type wallDiscoverer struct {
	provs []peer.AddrInfo
}

func (d wallDiscoverer) FindProviders(ctx context.Context, _ cid.Cid, _ int) ([]peer.AddrInfo, error) {
	<-ctx.Done()
	return d.provs, nil
}

func TestDiscovererFinderDeliversAfterWalkCut(t *testing.T) {
	provs := make([]peer.AddrInfo, 16)
	for i := range provs {
		provs[i] = peer.AddrInfo{ID: peer.ID(string(rune('a' + i)))}
	}
	f := discovererFinder{disc: wallDiscoverer{provs: provs}}

	// finder deadline as the PQM sets it; the walk must be cut early
	// enough that every found record still reaches the channel
	ctx, cancel := context.WithTimeout(context.Background(), walkEmitMargin+200*time.Millisecond)
	defer cancel()
	got := 0
	for range f.FindProvidersAsync(ctx, mustTestCID(t), 0) {
		got++
	}
	if got != len(provs) {
		t.Fatalf("delivered %d of %d records found before the cut; emission must not race the dead walk ctx", got, len(provs))
	}
}

func TestDiscovererFinderErrorClosesEmpty(t *testing.T) {
	f := discovererFinder{disc: staticDiscoverer{err: errors.New("boom")}}
	n := 0
	for range f.FindProvidersAsync(context.Background(), mustTestCID(t), 0) {
		n++
	}
	if n != 0 {
		t.Fatalf("error case streamed %d providers, want closed empty", n)
	}
}
