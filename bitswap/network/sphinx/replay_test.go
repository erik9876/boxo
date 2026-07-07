package sphinx

import (
	"fmt"
	"sync"
	"testing"
)

func TestReplayFilterTestAndSet(t *testing.T) {
	f := NewReplayFilter()
	tagA := []byte("tag-a")
	tagB := []byte("tag-b")

	if f.TestAndSet(tagA) {
		t.Fatal("fresh tag reported as seen")
	}
	if !f.TestAndSet(tagA) {
		t.Fatal("repeated tag not reported as seen")
	}
	if f.TestAndSet(tagB) {
		t.Fatal("distinct tag reported as seen")
	}
	if f.Len() != 2 {
		t.Errorf("Len = %d, want 2", f.Len())
	}
}

func TestReplayFilterConcurrent(t *testing.T) {
	f := NewReplayFilter()
	const goroutines = 32

	// All goroutines race on the same tag; exactly one may win
	var wg sync.WaitGroup
	firsts := make(chan struct{}, goroutines)
	for range goroutines {
		wg.Go(func() {
			if !f.TestAndSet([]byte("contested")) {
				firsts <- struct{}{}
			}
		})
	}
	wg.Wait()
	if len(firsts) != 1 {
		t.Errorf("%d goroutines saw the tag as fresh, want exactly 1", len(firsts))
	}

	// Distinct tags in parallel must all be fresh exactly once
	for i := range goroutines {
		wg.Go(func() {
			tag := fmt.Appendf(nil, "tag-%d", i)
			if f.TestAndSet(tag) {
				t.Errorf("distinct tag %d reported as seen", i)
			}
		})
	}
	wg.Wait()
}
