package main

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func TestClientSetInitiatorFirstThenHighSeqs(t *testing.T) {
	if got := clientSet(10, 0); len(got) != 0 {
		t.Errorf("clients=0 yields %v, want empty", got)
	}
	if got := clientSet(10, 1); !got[1] || len(got) != 1 {
		t.Errorf("clients=1 yields %v, want {1}", got)
	}
	got := clientSet(10, 3)
	for _, want := range []int64{1, 10, 9} {
		if !got[want] {
			t.Errorf("clients=3 misses seq %d (got %v)", want, got)
		}
	}
	if len(got) != 3 || got[2] {
		t.Errorf("clients=3 yields %v, want {1,10,9} and never the provider", got)
	}
}

func TestVictimSetSkipsClients(t *testing.T) {
	n, f := 12, 4
	clients := clientSet(n, 3) // {1, 12, 11}
	a := victimSet(7, n, f, clients)
	b := victimSet(7, n, f, clients)
	if len(a) != f {
		t.Fatalf("drew %d victims, want %d", len(a), f)
	}
	for seq := range a {
		if seq < 3 || seq > int64(n) {
			t.Errorf("victim seq %d outside pool range", seq)
		}
		if clients[seq] {
			t.Errorf("victim seq %d is a client", seq)
		}
		if !b[seq] {
			t.Errorf("victim set not deterministic: %v vs %v", a, b)
		}
	}
}

func TestDeriveBlocksDeterministic(t *testing.T) {
	b1, c1, err := deriveBlocks(7, 3)
	if err != nil {
		t.Fatalf("deriveBlocks: %v", err)
	}
	b2, c2, err := deriveBlocks(7, 3)
	if err != nil {
		t.Fatalf("deriveBlocks: %v", err)
	}
	if len(b1) != 3 || len(c1) != 3 {
		t.Fatalf("derived %d blocks / %d cids, want 3/3", len(b1), len(c1))
	}
	for i := range b1 {
		if !c1[i].Equals(c2[i]) {
			t.Errorf("cid %d differs across runs: %s vs %s", i, c1[i], c2[i])
		}
		if len(b1[i].RawData()) != 4096 {
			t.Errorf("block %d is %d bytes, want 4096", i, len(b1[i].RawData()))
		}
		if !b1[i].Cid().Equals(c1[i]) {
			t.Errorf("block %d cid mismatch", i)
		}
		if string(b1[i].RawData()) != string(b2[i].RawData()) {
			t.Errorf("block %d data differs across runs", i)
		}
	}
	_, c3, err := deriveBlocks(8, 3)
	if err != nil {
		t.Fatalf("deriveBlocks: %v", err)
	}
	if c1[0].Equals(c3[0]) {
		t.Error("different seeds produced the same cid")
	}

	if c1[0].Equals(c1[1]) {
		t.Error("blocks 0 and 1 of the same seed produced the same cid")
	}

	mh, err := multihash.Sum(b1[0].RawData(), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing block 0: %v", err)
	}
	wantCid := cid.NewCidV1(cid.Raw, mh)
	if !c1[0].Equals(wantCid) {
		t.Errorf("cid 0 = %s, want the cid recomputed from the block's raw data (%s)", c1[0], wantCid)
	}
}

func TestRosterVictimsMapsSeqsToPeers(t *testing.T) {
	roster := []rosterEntry{
		{Seq: 1, Info: peer.AddrInfo{ID: peer.ID("init")}},
		{Seq: 2, Info: peer.AddrInfo{ID: peer.ID("prov")}},
		{Seq: 3, Info: peer.AddrInfo{ID: peer.ID("p3")}},
		{Seq: 4, Info: peer.AddrInfo{ID: peer.ID("p4")}},
	}
	victims := map[int64]bool{3: true}
	vp, pp := rosterVictims(roster, victims)
	if len(vp) != 1 || vp[0] != peer.ID("p3").String() {
		t.Errorf("victim peers = %v, want [%s]", vp, peer.ID("p3").String())
	}
	if pp != peer.ID("prov").String() {
		t.Errorf("provider peer = %q, want %q", pp, peer.ID("prov").String())
	}
}
