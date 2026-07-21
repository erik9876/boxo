package sphinx

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// fakeJobStarter serves one canned StartJob outcome through the JobStarter
// seam
type fakeJobStarter struct {
	res chan JobResult
	err error
}

func (f *fakeJobStarter) StartJob(ctx context.Context, c cid.Cid) (<-chan JobResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
}

// settledJobStarter returns a fakeJobStarter whose result channel already
// holds r and is closed, the shape a settled JobManager channel has
func settledJobStarter(r JobResult) *fakeJobStarter {
	ch := make(chan JobResult, 1)
	ch <- r
	close(ch)
	return &fakeJobStarter{res: ch}
}

// collectProviders drains ch until it closes, failing the test after d
func collectProviders(t *testing.T, ch <-chan peer.AddrInfo, d time.Duration) []peer.AddrInfo {
	t.Helper()
	deadline := time.After(d)
	var out []peer.AddrInfo
	for {
		select {
		case p, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, p)
		case <-deadline:
			t.Fatal("provider channel never closed")
		}
	}
}

// TestProviderFinderStreamsProviders runs the adapter over the fully wired
// loopback stack: FindProvidersAsync in, the merged quorum providers out,
// channel closed
func TestProviderFinderStreamsProviders(t *testing.T) {
	want := testProviders(t, 3)
	initiator, _ := newJobCluster(t, &fakeDiscoverer{providers: want}, 20*time.Second)
	finder := NewProviderFinder(initiator.jobs)

	got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), 30*time.Second)
	assertProvidersEqual(t, got, want)
}

// TestProviderFinderFailureClosesEmpty pins the no-fallback semantics: a
// job that times out yields zero providers and a closed channel; the
// adapter never falls through to another discovery path, and the metrics
// count the outcome
func TestProviderFinderFailureClosesEmpty(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m, Timeout: 100 * time.Millisecond})
	finder := NewProviderFinder(jm)

	got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), 10*time.Second)
	if len(got) != 0 {
		t.Errorf("timed-out lookup yielded %d providers, want 0", len(got))
	}
	if met := jm.Metrics(); met.JobsTimedOut != 1 {
		t.Errorf("JobsTimedOut = %d, want 1", met.JobsTimedOut)
	}
}

// TestProviderFinderStartJobErrors covers failures before a job is even
// pending: a starved pool and a closed manager both yield an immediately
// closed, empty channel
func TestProviderFinderStartJobErrors(t *testing.T) {
	sender := &fakeSender{}
	k, m := DefaultBranchesPerJob, ReturnPathsPerJob
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m)-1, JobConfig{})
	finder := NewProviderFinder(jm)

	if got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), time.Second); len(got) != 0 {
		t.Errorf("pool-too-small lookup yielded %d providers, want 0", len(got))
	}

	jm.Close()
	if got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), time.Second); len(got) != 0 {
		t.Errorf("lookup on a closed manager yielded %d providers, want 0", len(got))
	}
}

// TestProviderFinderCtxCancel: a dying caller ctx stops the wait and
// closes the channel promptly. The job itself is not cancelled (the
// JobManager has no cancel API) and settles on its own timers; its result
// lands in the buffered channel unobserved
func TestProviderFinderCtxCancel(t *testing.T) {
	sender := &fakeSender{}
	k, m := 1, 1
	jm, _ := newTestJobManager(t, sender, jobSampleSize(k, m), JobConfig{Branches: k, ReturnPaths: m, Timeout: 30 * time.Second})
	finder := NewProviderFinder(jm)

	ctx, cancel := context.WithCancel(context.Background())
	ch := finder.FindProvidersAsync(ctx, testCID(t), 0)
	cancel()
	if got := collectProviders(t, ch, 5*time.Second); len(got) != 0 {
		t.Errorf("canceled lookup yielded %d providers, want 0", len(got))
	}

	// The job is still pending, untouched by the cancellation; settling it
	// now must not disturb anything (the result is simply unobserved)
	if jobs, _ := jm.pendingCounts(); jobs != 1 {
		t.Fatalf("pending jobs after cancel = %d, want 1 (job not cancelled)", jobs)
	}
	payload, err := EncodeReply(ReplyStatusOK, testProviders(t, 1))
	if err != nil {
		t.Fatalf("EncodeReply: %v", err)
	}
	jm.HandleSURBReply(jm.pendingSURBIDs()[0], payload)
	if jobs, surbIDs := jm.pendingCounts(); jobs != 0 || surbIDs != 0 {
		t.Errorf("state after late settle: %d jobs / %d surb ids", jobs, surbIDs)
	}
}

// TestProviderFinderHonorsCount pins the routing.ContentDiscovery count
// contract at the adapter: positive caps the stream, zero means all
func TestProviderFinderHonorsCount(t *testing.T) {
	want := testProviders(t, 3)

	finder := NewProviderFinder(settledJobStarter(JobResult{Providers: want}))
	if got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 1), time.Second); len(got) != 1 {
		t.Errorf("count=1 lookup yielded %d providers, want 1", len(got))
	}

	finder = NewProviderFinder(settledJobStarter(JobResult{Providers: want}))
	got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), time.Second)
	assertProvidersEqual(t, got, want)
}

// TestProviderFinderReportsJobError: a settled failure (here the proxy's
// Failed status) yields zero providers and a closed channel
func TestProviderFinderReportsJobError(t *testing.T) {
	finder := NewProviderFinder(settledJobStarter(JobResult{Err: ErrDiscoveryFailed}))
	if got := collectProviders(t, finder.FindProvidersAsync(context.Background(), testCID(t), 0), time.Second); len(got) != 0 {
		t.Errorf("failed lookup yielded %d providers, want 0", len(got))
	}
}

func TestSuggestedFinderTimeout(t *testing.T) {
	if got, want := SuggestedFinderTimeout(JobConfig{}), 2*DefaultJobTimeout+5*time.Second; got != want {
		t.Errorf("zero-value timeout = %s, want %s", got, want)
	}
	if got, want := SuggestedFinderTimeout(JobConfig{Timeout: 10 * time.Second}), 25*time.Second; got != want {
		t.Errorf("explicit timeout = %s, want %s", got, want)
	}
}
