package sphinx

import (
	"bytes"
	"testing"
)

func TestSURBStorePutGetDelete(t *testing.T) {
	s := NewSURBStore()
	id := SURBID{1}
	keys := []byte{10, 20, 30}

	if _, ok := s.Get(id); ok {
		t.Fatal("Get on empty store returned an entry")
	}

	s.Put(id, keys)
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
	got, ok := s.Get(id)
	if !ok || !bytes.Equal(got, keys) {
		t.Fatalf("Get = %v, %v; want %v, true", got, ok, keys)
	}

	s.Delete(id)
	if _, ok := s.Get(id); ok {
		t.Fatal("entry survived Delete")
	}
	if s.Len() != 0 {
		t.Fatalf("Len after Delete = %d, want 0", s.Len())
	}
}

func TestSURBStoreCopySemantics(t *testing.T) {
	s := NewSURBStore()
	id := SURBID{2}
	keys := []byte{1, 2, 3}

	// Put must copy: mutating the input afterwards may not reach the store
	s.Put(id, keys)
	keys[0] = 99
	got, _ := s.Get(id)
	if got[0] != 1 {
		t.Error("mutating the Put input changed the stored keys")
	}

	// Get must copy: zeroing the returned slice (as DecryptSURBPayload
	// does) may not destroy the stored entry
	for i := range got {
		got[i] = 0
	}
	again, ok := s.Get(id)
	if !ok || !bytes.Equal(again, []byte{1, 2, 3}) {
		t.Error("mutating a Get result changed the stored keys")
	}
}
