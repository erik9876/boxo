package sphinx

import "sync"

// SURBStore maps outstanding SURB IDs to the keys needed to open their
// replies. Reply matching, timers and cleanup are the job layer's business
type SURBStore struct {
	mu   sync.Mutex
	keys map[SURBID][]byte
}

func NewSURBStore() *SURBStore {
	return &SURBStore{keys: make(map[SURBID][]byte)}
}

func (s *SURBStore) Put(id SURBID, keys []byte) {
	cp := append([]byte(nil), keys...)
	s.mu.Lock()
	s.keys[id] = cp
	s.mu.Unlock()
}

// Get returns a copy: DecryptSURBPayload zeroes the keys it is handed, the
// stored entry survives until Delete
func (s *SURBStore) Get(id SURBID) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, ok := s.keys[id]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), keys...), true
}

// a SURB is single-use
func (s *SURBStore) Delete(id SURBID) {
	s.mu.Lock()
	delete(s.keys, id)
	s.mu.Unlock()
}

func (s *SURBStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}
