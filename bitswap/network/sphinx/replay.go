package sphinx

import "sync"

// ReplayFilter is an in-memory set of replay tags; a repeated tag means a
// replayed packet. Scoped to the key epoch = process lifetime: after a
// restart old packets are undecryptable anyway, so starting empty is
// correct and nothing needs persisting
type ReplayFilter struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewReplayFilter() *ReplayFilter {
	return &ReplayFilter{seen: make(map[string]struct{})}
}

// TestAndSet records tag and reports whether it was seen before, in one
// atomic step. Only tags of successfully unwrapped packets belong here:
// recording failing ones would let a tampered copy burn the real packet's
// tag
func (f *ReplayFilter) TestAndSet(tag []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.seen[string(tag)]; ok {
		return true
	}
	f.seen[string(tag)] = struct{}{}
	return false
}

func (f *ReplayFilter) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
}
