package sphinx

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// base is a fixed reference instant used to make clock behaviour deterministic
var base = time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)

// newStoreAt returns a KeyStore whose clock is pinned to at
func newStoreAt(at time.Time) *KeyStore {
	ks := NewKeyStore()
	ks.now = func() time.Time { return at }
	return ks
}

// makeRecord builds a KeyRecord for a fresh identity with the given seq and
// expiry. It returns the record and its peer ID
func makeRecord(t *testing.T, seq uint64, expiry time.Time) (*KeyRecord, peer.ID) {
	t.Helper()
	_, pid := newIdentity(t)
	rec := &KeyRecord{
		PeerID:          pid,
		SphinxPublicKey: newSphinxKey(t).Bytes(),
		Seq:             seq,
		Expiry:          expiry,
	}
	return rec, pid
}

func TestKeyStorePutGet(t *testing.T) {
	ks := newStoreAt(base)
	rec, pid := makeRecord(t, 1, base.Add(time.Hour))

	if err := ks.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	info, ok := ks.Get(pid)
	if !ok {
		t.Fatal("Get: entry not found after Put")
	}
	if info.PeerID != pid {
		t.Errorf("Get returned entry for %s, want %s", info.PeerID, pid)
	}
	assertRecordsEqual(t, rec, info.Record)
	if info.PublicKey == nil {
		t.Error("Get returned nil parsed public key")
	}
}

func TestKeyStorePutStoresCopy(t *testing.T) {
	ks := newStoreAt(base)
	rec, pid := makeRecord(t, 1, base.Add(time.Hour))
	want := *rec
	want.SphinxPublicKey = append([]byte(nil), rec.SphinxPublicKey...)

	if err := ks.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Mutating the caller's record must not affect the stored entry
	rec.SphinxPublicKey[0] ^= 0xFF
	rec.Seq = 99
	rec.Expiry = base.Add(-time.Hour)

	info, ok := ks.Get(pid)
	if !ok {
		t.Fatal("Get: entry not found after Put")
	}
	assertRecordsEqual(t, &want, info.Record)
}

func TestKeyStoreRejectsLowerOrEqualSeq(t *testing.T) {
	ks := newStoreAt(base)
	high, pid := makeRecord(t, 10, base.Add(time.Hour))
	if err := ks.Put(high); err != nil {
		t.Fatalf("Put high seq: %v", err)
	}

	// A record for the same peer with a lower seq must be refused
	lower := &KeyRecord{PeerID: pid, SphinxPublicKey: newSphinxKey(t).Bytes(), Seq: 9, Expiry: base.Add(time.Hour)}
	if err := ks.Put(lower); err == nil {
		t.Error("Put with lower seq: got nil error, want rejection")
	}
	// Equal seq must also be refused (strictly-increasing rule)
	equal := &KeyRecord{PeerID: pid, SphinxPublicKey: newSphinxKey(t).Bytes(), Seq: 10, Expiry: base.Add(time.Hour)}
	if err := ks.Put(equal); err == nil {
		t.Error("Put with equal seq: got nil error, want rejection")
	}

	// The originally stored high-seq record must still be the one held
	info, ok := ks.Get(pid)
	if !ok {
		t.Fatal("Get: entry missing")
	}
	if info.Record.Seq != 10 {
		t.Errorf("stored seq = %d, want 10", info.Record.Seq)
	}

	// A strictly higher seq must be accepted
	higher := &KeyRecord{PeerID: pid, SphinxPublicKey: newSphinxKey(t).Bytes(), Seq: 11, Expiry: base.Add(time.Hour)}
	if err := ks.Put(higher); err != nil {
		t.Errorf("Put with higher seq: %v", err)
	}
	if info, _ := ks.Get(pid); info.Record.Seq != 11 {
		t.Errorf("stored seq after higher Put = %d, want 11", info.Record.Seq)
	}
}

func TestKeyStoreRejectsExpiredOnPut(t *testing.T) {
	ks := newStoreAt(base)
	expired, pid := makeRecord(t, 1, base.Add(-time.Minute)) // already in the past

	if err := ks.Put(expired); err == nil {
		t.Error("Put of expired record: got nil error, want rejection")
	}
	if _, ok := ks.Get(pid); ok {
		t.Error("expired record was stored despite rejection")
	}
}

func TestKeyStoreGetEvictsExpired(t *testing.T) {
	ks := newStoreAt(base)
	rec, pid := makeRecord(t, 1, base.Add(time.Minute))
	if err := ks.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Advance the clock past the expiry
	ks.now = func() time.Time { return base.Add(2 * time.Minute) }

	if _, ok := ks.Get(pid); ok {
		t.Error("Get returned an expired entry")
	}
	if ks.Len() != 0 {
		t.Errorf("Len = %d after Get evicted expired entry, want 0", ks.Len())
	}
}

func TestKeyStoreSampleNoDuplicatesNeverExpired(t *testing.T) {
	ks := newStoreAt(base)

	const liveCount = 8
	live := make(map[peer.ID]bool, liveCount)
	for range liveCount {
		rec, pid := makeRecord(t, 1, base.Add(time.Hour))
		if err := ks.Put(rec); err != nil {
			t.Fatalf("Put live: %v", err)
		}
		live[pid] = true
	}
	// Add records that will be expired at sample time by inserting them while
	// the clock is in the past, then advancing it
	past := base.Add(-time.Hour)
	ks.now = func() time.Time { return past }
	for range 4 {
		rec, _ := makeRecord(t, 1, past.Add(time.Minute)) // expires before base
		if err := ks.Put(rec); err != nil {
			t.Fatalf("Put soon-expired: %v", err)
		}
	}
	ks.now = func() time.Time { return base }

	// Repeat to exercise the randomness and eviction paths
	for range 20 {
		got := ks.Sample(5)
		if len(got) != 5 {
			t.Fatalf("Sample(5) returned %d entries, want 5", len(got))
		}
		seen := make(map[peer.ID]bool, len(got))
		for _, info := range got {
			if seen[info.PeerID] {
				t.Errorf("Sample returned duplicate peer %s", info.PeerID)
			}
			seen[info.PeerID] = true
			if !live[info.PeerID] {
				t.Errorf("Sample returned unexpected (expired?) peer %s", info.PeerID)
			}
			if info.Record.IsExpired(base) {
				t.Errorf("Sample returned expired record for %s", info.PeerID)
			}
		}
	}

	// Requesting more than available returns exactly the live set
	if got := ks.Sample(100); len(got) != liveCount {
		t.Errorf("Sample(100) returned %d, want %d live entries", len(got), liveCount)
	}
	// The expired entries must have been evicted along the way
	if ks.Len() != liveCount {
		t.Errorf("Len = %d after sampling, want %d (expired evicted)", ks.Len(), liveCount)
	}
}

func TestKeyStoreConcurrentAccess(t *testing.T) {
	ks := NewKeyStore() // real clock

	// Pre-generate records in the test goroutine: several peers, several seqs
	// each, so concurrent Puts hit both the insert and the stale-seq path
	const numPeers = 4
	const seqsPerPeer = 8
	expiry := time.Now().Add(time.Hour)
	pids := make([]peer.ID, numPeers)
	recs := make([]*KeyRecord, 0, numPeers*seqsPerPeer)
	for i := range numPeers {
		_, pid := newIdentity(t)
		pids[i] = pid
		pub := newSphinxKey(t).Bytes()
		for seq := uint64(1); seq <= seqsPerPeer; seq++ {
			recs = append(recs, &KeyRecord{
				PeerID:          pid,
				SphinxPublicKey: pub,
				Seq:             seq,
				Expiry:          expiry,
			})
		}
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(2)
		go func() { // writer: stale-seq rejections are expected
			defer wg.Done()
			for _, rec := range recs {
				_ = ks.Put(rec)
			}
		}()
		go func() { // reader
			defer wg.Done()
			for i := range 200 {
				ks.Get(pids[i%numPeers])
				ks.Sample(3)
				ks.Len()
			}
		}()
	}
	wg.Wait()

	// Every peer must end up stored with its highest seq
	for _, pid := range pids {
		info, ok := ks.Get(pid)
		if !ok {
			t.Fatalf("no record for %s after concurrent puts", pid)
		}
		if info.Record.Seq != seqsPerPeer {
			t.Errorf("stored seq for %s = %d, want %d", pid, info.Record.Seq, uint64(seqsPerPeer))
		}
	}
}

func TestSampleExcluding(t *testing.T) {
	ks := newStoreAt(base)

	const total = 10
	pids := make([]peer.ID, 0, total)
	for range total {
		rec, pid := makeRecord(t, 1, base.Add(time.Hour))
		if err := ks.Put(rec); err != nil {
			t.Fatalf("Put: %v", err)
		}
		pids = append(pids, pid)
	}
	exclude := map[peer.ID]struct{}{}
	for _, pid := range pids[:4] {
		exclude[pid] = struct{}{}
	}

	// Excluded peers never appear; repeat to exercise the randomness
	for range 20 {
		for _, info := range ks.SampleExcluding(5, exclude) {
			if _, banned := exclude[info.PeerID]; banned {
				t.Fatalf("SampleExcluding returned excluded peer %s", info.PeerID)
			}
		}
	}

	// n over the remainder returns exactly the non-excluded live set
	got := ks.SampleExcluding(100, exclude)
	if len(got) != total-len(exclude) {
		t.Errorf("SampleExcluding(100) returned %d entries, want %d", len(got), total-len(exclude))
	}
	seen := make(map[peer.ID]bool, len(got))
	for _, info := range got {
		if seen[info.PeerID] {
			t.Errorf("SampleExcluding returned duplicate peer %s", info.PeerID)
		}
		seen[info.PeerID] = true
	}

	// Excluding everything yields nothing
	all := map[peer.ID]struct{}{}
	for _, pid := range pids {
		all[pid] = struct{}{}
	}
	if got := ks.SampleExcluding(3, all); got != nil {
		t.Errorf("SampleExcluding with full exclusion = %v, want nil", got)
	}

	// nil and empty exclusions behave like Sample
	if got := ks.SampleExcluding(100, nil); len(got) != total {
		t.Errorf("SampleExcluding(100, nil) returned %d entries, want %d", len(got), total)
	}
	if got := ks.SampleExcluding(100, map[peer.ID]struct{}{}); len(got) != total {
		t.Errorf("SampleExcluding(100, empty) returned %d entries, want %d", len(got), total)
	}

	// Lazy eviction still runs, excluded or not
	ks.now = func() time.Time { return base.Add(2 * time.Hour) }
	if got := ks.SampleExcluding(100, exclude); got != nil {
		t.Errorf("SampleExcluding after expiry = %v, want nil", got)
	}
	if ks.Len() != 0 {
		t.Errorf("Len = %d after expired sampling, want 0 (lazy eviction)", ks.Len())
	}
}

func TestKeyStoreSampleEmptyAndNonPositive(t *testing.T) {
	ks := newStoreAt(base)
	if got := ks.Sample(3); got != nil {
		t.Errorf("Sample on empty store = %v, want nil", got)
	}
	rec, _ := makeRecord(t, 1, base.Add(time.Hour))
	if err := ks.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := ks.Sample(0); got != nil {
		t.Errorf("Sample(0) = %v, want nil", got)
	}
}
