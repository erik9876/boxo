package sphinx

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/katzenpost/hpqc/nike"
	"github.com/libp2p/go-libp2p/core/peer"
)

// the store already holds a record at least as fresh
var ErrStaleRecord = errors.New("key record is not newer than the stored one")

// KeyInfo couples a stored KeyRecord with its parsed public key
type KeyInfo struct {
	PeerID    peer.ID
	Record    *KeyRecord
	PublicKey nike.PublicKey
}

// KeyStore holds the freshest known KeyRecord per peer and doubles as the
// relay pool. Rejects stale and expired records on insert, never hands out
// expired entries (lazy eviction)
type KeyStore struct {
	mu      sync.Mutex
	entries map[peer.ID]KeyInfo

	// clock, overridable in tests
	now func() time.Time
}

func NewKeyStore() *KeyStore {
	return &KeyStore{
		entries: make(map[peer.ID]KeyInfo),
		now:     time.Now,
	}
}

// Put rejects nil, unparseable keys, expired records and any Seq not
// strictly above the stored one. Keeps a private copy
func (ks *KeyStore) Put(rec *KeyRecord) error {
	if rec == nil {
		return fmt.Errorf("cannot store nil KeyRecord")
	}
	cp := *rec
	cp.SphinxPublicKey = append([]byte(nil), rec.SphinxPublicKey...)
	pub, err := cp.PublicKey()
	if err != nil {
		return fmt.Errorf("parsing sphinx public key for %s: %w", cp.PeerID, err)
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	if cp.IsExpired(ks.now()) {
		return fmt.Errorf("refusing expired KeyRecord for %s", cp.PeerID)
	}
	if cur, ok := ks.entries[cp.PeerID]; ok && cp.Seq <= cur.Record.Seq {
		return fmt.Errorf("refusing KeyRecord for %s: seq %d <= stored seq %d: %w",
			cp.PeerID, cp.Seq, cur.Record.Seq, ErrStaleRecord)
	}

	ks.entries[cp.PeerID] = KeyInfo{PeerID: cp.PeerID, Record: &cp, PublicKey: pub}
	return nil
}

func (ks *KeyStore) Get(pid peer.ID) (KeyInfo, bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	info, ok := ks.entries[pid]
	if !ok {
		return KeyInfo{}, false
	}
	if info.Record.IsExpired(ks.now()) {
		delete(ks.entries, pid)
		return KeyInfo{}, false
	}
	return info, true
}

// Sample draws up to n distinct live entries, uniform without replacement,
// crypto/rand (relay selection must be unpredictable). nil when n <= 0 or
// the pool is empty
func (ks *KeyStore) Sample(n int) []KeyInfo {
	return ks.SampleExcluding(n, nil)
}

// SampleExcluding is Sample minus the excluded peers; nil/empty exclusion
// behaves like Sample. Falling back to an unrestricted draw on a starved
// sample is the caller's call
func (ks *KeyStore) SampleExcluding(n int, exclude map[peer.ID]struct{}) []KeyInfo {
	if n <= 0 {
		return nil
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	now := ks.now()
	live := make([]KeyInfo, 0, len(ks.entries))
	for pid, info := range ks.entries {
		if info.Record.IsExpired(now) {
			delete(ks.entries, pid)
			continue
		}
		if _, excluded := exclude[pid]; excluded {
			continue
		}
		live = append(live, info)
	}

	if n > len(live) {
		n = len(live)
	}
	if n == 0 {
		return nil
	}
	// partial Fisher-Yates over the first n slots
	for i := 0; i < n; i++ {
		j := i + randIntn(len(live)-i)
		live[i], live[j] = live[j], live[i]
	}
	return live[:n:n]
}

// EvictExpired removes all expired entries, returns the count
func (ks *KeyStore) EvictExpired() int {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	now := ks.now()
	evicted := 0
	for pid, info := range ks.entries {
		if info.Record.IsExpired(now) {
			delete(ks.entries, pid)
			evicted++
		}
	}
	return evicted
}

// Len includes expired entries not yet evicted
func (ks *KeyStore) Len() int {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	return len(ks.entries)
}

// panics on CSPRNG failure; silently weak randomness would be worse for
// path selection than a crash
func randIntn(n int) int {
	if n <= 0 {
		return 0
	}
	v, err := crand.Int(crand.Reader, big.NewInt(int64(n)))
	if err != nil {
		panic(fmt.Sprintf("sphinx: crypto/rand failure during sampling: %v", err))
	}
	return int(v.Int64())
}
