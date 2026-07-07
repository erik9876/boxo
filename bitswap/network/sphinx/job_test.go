package sphinx

import (
	"bytes"
	crand "crypto/rand"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

// testCID returns a realistic CIDv1 (raw, sha2-256; 36 bytes)
func testCID(t *testing.T) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte("the hidden path"), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("hashing test cid: %v", err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// identityCID returns a valid CID whose binary form is exactly totalLen
// bytes, via an identity multihash: 1 (version) + 1 (codec) + 2 (multihash
// header) + digest
func identityCID(t *testing.T, totalLen int) cid.Cid {
	t.Helper()
	digest := make([]byte, totalLen-4)
	h, err := mh.Sum(digest, mh.IDENTITY, -1)
	if err != nil {
		t.Fatalf("building identity multihash: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, h)
	if len(c.Bytes()) != totalLen {
		t.Fatalf("identity cid is %d bytes, want %d", len(c.Bytes()), totalLen)
	}
	return c
}

// randomSURBs returns m byte blobs of exactly SURBLength. The codec treats
// SURB contents as opaque, so random bytes suffice
func randomSURBs(t *testing.T, m int) [][]byte {
	t.Helper()
	surbLen := Geometry().SURBLength
	surbs := make([][]byte, m)
	for i := range surbs {
		surbs[i] = make([]byte, surbLen)
		if _, err := crand.Read(surbs[i]); err != nil {
			t.Fatalf("generating random surb: %v", err)
		}
	}
	return surbs
}

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("parsing multiaddr %q: %v", s, err)
	}
	return a
}

// testProviders returns a small provider list well within the wire caps
func testProviders(t *testing.T, n int) []peer.AddrInfo {
	t.Helper()
	providers := make([]peer.AddrInfo, n)
	for i := range providers {
		_, pid := newIdentity(t)
		providers[i] = peer.AddrInfo{ID: pid}
		switch i % 3 {
		case 0: // no addrs: a bare provider entry is valid
		case 1:
			providers[i].Addrs = []ma.Multiaddr{mustAddr(t, "/ip4/10.0.0.1/tcp/4001")}
		case 2:
			providers[i].Addrs = []ma.Multiaddr{
				mustAddr(t, "/ip4/10.0.0.2/udp/4001/quic-v1"),
				mustAddr(t, "/ip6/2001:db8::1/tcp/4001"),
			}
		}
	}
	return providers
}

// assertProvidersEqual compares provider lists byte-exactly: same order,
// same peer IDs, same multiaddr bytes
func assertProvidersEqual(t *testing.T, got, want []peer.AddrInfo) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d providers, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Errorf("provider %d: id = %s, want %s", i, got[i].ID, want[i].ID)
		}
		if len(got[i].Addrs) != len(want[i].Addrs) {
			t.Errorf("provider %d: %d addrs, want %d", i, len(got[i].Addrs), len(want[i].Addrs))
			continue
		}
		for j := range want[i].Addrs {
			if !bytes.Equal(got[i].Addrs[j].Bytes(), want[i].Addrs[j].Bytes()) {
				t.Errorf("provider %d addr %d: %s, want %s", i, j, got[i].Addrs[j], want[i].Addrs[j])
			}
		}
	}
}

func TestJobCodecRoundtrip(t *testing.T) {
	for _, m := range []int{1, 2, ReturnPathsPerJob} {
		c := testCID(t)
		surbs := randomSURBs(t, m)
		payload, err := EncodeJob(c, surbs)
		if err != nil {
			t.Fatalf("m=%d: EncodeJob: %v", m, err)
		}
		job, err := DecodeJob(payload)
		if err != nil {
			t.Fatalf("m=%d: DecodeJob: %v", m, err)
		}
		if !job.CID.Equals(c) {
			t.Errorf("m=%d: cid = %s, want %s", m, job.CID, c)
		}
		if len(job.SURBs) != m {
			t.Fatalf("m=%d: decoded %d surbs", m, len(job.SURBs))
		}
		for i := range surbs {
			if !bytes.Equal(job.SURBs[i], surbs[i]) {
				t.Errorf("m=%d: surb %d altered in roundtrip", m, i)
			}
		}
	}
}

func TestJobBudgetEdges(t *testing.T) {
	surbLen := Geometry().SURBLength
	// The largest m the budget can ever hold, with the CID length that
	// makes the job land exactly on the 2048 B budget
	m := (UserPayloadLength - jobHeaderLen - 1) / surbLen
	exactCIDLen := UserPayloadLength - jobHeaderLen - m*surbLen

	payload, err := EncodeJob(identityCID(t, exactCIDLen), randomSURBs(t, m))
	if err != nil {
		t.Fatalf("EncodeJob at exactly %d bytes: %v", UserPayloadLength, err)
	}
	if len(payload) != UserPayloadLength {
		t.Fatalf("edge job is %d bytes, want exactly %d", len(payload), UserPayloadLength)
	}
	if _, err := DecodeJob(payload); err != nil {
		t.Fatalf("DecodeJob at exactly %d bytes: %v", UserPayloadLength, err)
	}

	// One byte over: same m, CID one byte longer
	if _, err := EncodeJob(identityCID(t, exactCIDLen+1), randomSURBs(t, m)); err == nil {
		t.Fatal("EncodeJob accepted a job one byte over the budget")
	}
	// The decoder enforces the budget independently of structure
	if _, err := DecodeJob(make([]byte, UserPayloadLength+1)); err == nil {
		t.Fatal("DecodeJob accepted a payload over the budget")
	}
}

func TestJobEncodeRejects(t *testing.T) {
	surbs := randomSURBs(t, 2)
	if _, err := EncodeJob(cid.Undef, surbs); err == nil {
		t.Error("EncodeJob accepted an undefined cid")
	}
	if _, err := EncodeJob(identityCID(t, MaxCIDLen+1), surbs); err == nil {
		t.Error("EncodeJob accepted an oversized cid")
	}
	if _, err := EncodeJob(testCID(t), nil); err == nil {
		t.Error("EncodeJob accepted a job without surbs")
	}
	short := [][]byte{surbs[0][:len(surbs[0])-1]}
	if _, err := EncodeJob(testCID(t), short); err == nil {
		t.Error("EncodeJob accepted a surb of the wrong length")
	}
}

func TestJobDecodeRejects(t *testing.T) {
	surbLen := Geometry().SURBLength
	valid, err := EncodeJob(testCID(t), randomSURBs(t, 2))
	if err != nil {
		t.Fatalf("EncodeJob: %v", err)
	}

	mutate := func(f func(p []byte) []byte) []byte {
		return f(append([]byte(nil), valid...))
	}
	cases := map[string][]byte{
		"empty":            {},
		"header only":      valid[:2],
		"zero command":     mutate(func(p []byte) []byte { p[0] = 0x00; return p }),
		"unknown command":  mutate(func(p []byte) []byte { p[0] = 0x7f; return p }),
		"zero cid length":  mutate(func(p []byte) []byte { p[1] = 0; return p }),
		"cid length > cap": mutate(func(p []byte) []byte { p[1] = MaxCIDLen + 1; return p }),
		"cid length beyond buffer": mutate(func(p []byte) []byte {
			return append(p[:8], 120) // claims a 120-byte cid, buffer ends first
		}),
		"unparseable cid": mutate(func(p []byte) []byte {
			for i := 2; i < 2+int(p[1]); i++ {
				p[i] = 0xff
			}
			return p
		}),
		"zero surbs": mutate(func(p []byte) []byte { p[2+int(p[1])] = 0; return p }),
		"lying surb count": mutate(func(p []byte) []byte {
			p[2+int(p[1])]++ // claims 3 surbs, carries 2
			return p
		}),
		"truncated surb": mutate(func(p []byte) []byte { return p[:len(p)-1] }),
		"trailing byte":  mutate(func(p []byte) []byte { return append(p, 0x00) }),
		"surb bytes not a multiple": mutate(func(p []byte) []byte {
			return append(p, make([]byte, surbLen/2)...)
		}),
	}
	for name, payload := range cases {
		if _, err := DecodeJob(payload); err == nil {
			t.Errorf("%s: DecodeJob accepted the payload", name)
		}
	}
	if _, err := DecodeJob(valid); err != nil {
		t.Fatalf("control: valid job rejected: %v", err)
	}
}

func TestReplyCodecRoundtrip(t *testing.T) {
	for _, n := range []int{0, 1, 3} {
		want := testProviders(t, n)
		payload, err := EncodeReply(ReplyStatusOK, want)
		if err != nil {
			t.Fatalf("n=%d: EncodeReply: %v", n, err)
		}
		reply, err := DecodeReply(payload)
		if err != nil {
			t.Fatalf("n=%d: DecodeReply: %v", n, err)
		}
		if reply.Status != ReplyStatusOK {
			t.Errorf("n=%d: status = 0x%02x, want ok", n, byte(reply.Status))
		}
		assertProvidersEqual(t, reply.Providers, want)
	}

	payload, err := EncodeReply(ReplyStatusFailed, nil)
	if err != nil {
		t.Fatalf("EncodeReply(failed): %v", err)
	}
	reply, err := DecodeReply(payload)
	if err != nil {
		t.Fatalf("DecodeReply(failed): %v", err)
	}
	if reply.Status != ReplyStatusFailed || len(reply.Providers) != 0 {
		t.Errorf("failed reply decoded to status 0x%02x with %d providers", byte(reply.Status), len(reply.Providers))
	}
}

// replyEntry hand-crafts one provider entry for decode-reject cases
func replyEntry(pid []byte, addrs ...[]byte) []byte {
	e := []byte{byte(len(pid))}
	e = append(e, pid...)
	e = append(e, byte(len(addrs)))
	for _, a := range addrs {
		e = append(e, byte(len(a)))
		e = append(e, a...)
	}
	return e
}

func TestReplyDecodeRejects(t *testing.T) {
	_, pid := newIdentity(t)
	pidB := []byte(pid)
	addrB := mustAddr(t, "/ip4/10.0.0.1/tcp/4001").Bytes()
	ok := byte(ReplyStatusOK)
	failed := byte(ReplyStatusFailed)

	cases := map[string][]byte{
		"empty":                 {},
		"status only":           {ok},
		"zero status":           append([]byte{0x00, 1}, replyEntry(pidB, addrB)...),
		"unknown status":        append([]byte{0x03, 1}, replyEntry(pidB, addrB)...),
		"failed with providers": append([]byte{failed, 1}, replyEntry(pidB, addrB)...),
		"count over cap":        {ok, MaxProvidersPerReply + 1},
		"lying provider count":  {ok, 1},
		"zero pid length":       append([]byte{ok, 1}, replyEntry(nil, addrB)...),
		"pid length over cap":   append([]byte{ok, 1}, replyEntry(make([]byte, MaxPeerIDLen+1), addrB)...),
		"truncated inside pid":  append([]byte{ok, 1}, replyEntry(pidB, addrB)[:10]...),
		"unparseable pid":       append([]byte{ok, 1}, replyEntry(bytes.Repeat([]byte{0xff}, 5), addrB)...),
		"addr count over cap":   append([]byte{ok, 1}, replyEntry(pidB, addrB, addrB, addrB)...),
		"zero addr length":      append([]byte{ok, 1}, replyEntry(pidB, nil)...),
		"addr length over cap":  append([]byte{ok, 1}, replyEntry(pidB, make([]byte, MaxAddrLen+1))...),
		"truncated inside addr": append([]byte{ok, 1}, replyEntry(pidB, addrB)[:len(replyEntry(pidB, addrB))-2]...),
		"unparseable addr":      append([]byte{ok, 1}, replyEntry(pidB, bytes.Repeat([]byte{0xff}, 6))...),
		"trailing byte":         append(append([]byte{ok, 1}, replyEntry(pidB, addrB)...), 0x00),
	}
	for name, payload := range cases {
		if _, err := DecodeReply(payload); err == nil {
			t.Errorf("%s: DecodeReply accepted the payload", name)
		}
	}
	control := append([]byte{ok, 1}, replyEntry(pidB, addrB)...)
	if _, err := DecodeReply(control); err != nil {
		t.Fatalf("control: valid reply rejected: %v", err)
	}
}

func TestEncodeReplyRejects(t *testing.T) {
	_, pid := newIdentity(t)
	small := mustAddr(t, "/ip4/10.0.0.1/tcp/4001")

	if _, err := EncodeReply(ReplyStatusOK, testProviders(t, MaxProvidersPerReply+1)); err == nil {
		t.Error("EncodeReply accepted a list over MaxProvidersPerReply")
	}
	if _, err := EncodeReply(ReplyStatusFailed, testProviders(t, 1)); err == nil {
		t.Error("EncodeReply accepted a failed reply with providers")
	}
	if _, err := EncodeReply(ReplyStatus(0x7f), nil); err == nil {
		t.Error("EncodeReply accepted an unknown status")
	}
	oversizedPid := peer.ID(strings.Repeat("x", MaxPeerIDLen+1))
	if _, err := EncodeReply(ReplyStatusOK, []peer.AddrInfo{{ID: oversizedPid}}); err == nil {
		t.Error("EncodeReply accepted an oversized peer id")
	}
	if _, err := EncodeReply(ReplyStatusOK, []peer.AddrInfo{{ID: ""}}); err == nil {
		t.Error("EncodeReply accepted an empty peer id")
	}
	tooManyAddrs := peer.AddrInfo{ID: pid, Addrs: []ma.Multiaddr{small, small, small}}
	if _, err := EncodeReply(ReplyStatusOK, []peer.AddrInfo{tooManyAddrs}); err == nil {
		t.Error("EncodeReply accepted more addrs than MaxAddrsPerProvider")
	}
	longAddr := mustAddr(t, "/dns4/"+strings.Repeat("a", 80)+".example.com/tcp/4001")
	if _, err := EncodeReply(ReplyStatusOK, []peer.AddrInfo{{ID: pid, Addrs: []ma.Multiaddr{longAddr}}}); err == nil {
		t.Error("EncodeReply accepted an oversized multiaddr")
	}
}

// worstCaseProvider builds an entry at exactly maxProviderEntryLen: a
// 42-byte peer ID (identity multihash over 40 bytes) and MaxAddrsPerProvider
// addresses of exactly MaxAddrLen bytes (a 38-char dns4 name encodes to 40
// bytes; the codec's caps are mechanical byte limits, so a dns4 addr at the
// cap is fine here even though the cap is sized for IP literals)
func worstCaseProvider(t *testing.T, seed byte) peer.AddrInfo {
	t.Helper()
	raw := bytes.Repeat([]byte{seed}, MaxPeerIDLen-2)
	h, err := mh.Sum(raw, mh.IDENTITY, -1)
	if err != nil {
		t.Fatalf("identity multihash: %v", err)
	}
	pid, err := peer.IDFromBytes(h)
	if err != nil {
		t.Fatalf("peer id from identity multihash: %v", err)
	}
	if len([]byte(pid)) != MaxPeerIDLen {
		t.Fatalf("worst-case pid is %d bytes, want %d", len([]byte(pid)), MaxPeerIDLen)
	}
	addrs := make([]ma.Multiaddr, MaxAddrsPerProvider)
	for i := range addrs {
		name := strings.Repeat(string(rune('a'+i)), MaxAddrLen-2)
		addrs[i] = mustAddr(t, "/dns4/"+name)
		if len(addrs[i].Bytes()) != MaxAddrLen {
			t.Fatalf("worst-case addr is %d bytes, want %d", len(addrs[i].Bytes()), MaxAddrLen)
		}
	}
	return peer.AddrInfo{ID: pid, Addrs: addrs}
}

// TestMaxProvidersPerReplyPinned pins the MaxProvidersPerReply derivation:
// the constant itself, the arithmetic behind it, and the guarantee that a
// full worst-case reply encodes within the budget while one more entry
// would not
func TestMaxProvidersPerReplyPinned(t *testing.T) {
	if MaxProvidersPerReply != 16 {
		t.Fatalf("MaxProvidersPerReply = %d, want 16; the wire caps changed, re-derive and update docs", MaxProvidersPerReply)
	}
	if maxProviderEntryLen != 126 {
		t.Fatalf("maxProviderEntryLen = %d, want 126", maxProviderEntryLen)
	}
	if worst := replyHeaderLen + MaxProvidersPerReply*maxProviderEntryLen; worst > UserPayloadLength {
		t.Fatalf("worst-case full reply is %d bytes, over the %d budget", worst, UserPayloadLength)
	}
	if overflow := replyHeaderLen + (MaxProvidersPerReply+1)*maxProviderEntryLen; overflow <= UserPayloadLength {
		t.Fatalf("one entry more would still fit (%d ≤ %d); MaxProvidersPerReply is not maximal", overflow, UserPayloadLength)
	}

	providers := make([]peer.AddrInfo, MaxProvidersPerReply)
	for i := range providers {
		providers[i] = worstCaseProvider(t, byte(i))
	}
	payload, err := EncodeReply(ReplyStatusOK, providers)
	if err != nil {
		t.Fatalf("EncodeReply of a full worst-case reply: %v", err)
	}
	if want := replyHeaderLen + MaxProvidersPerReply*maxProviderEntryLen; len(payload) != want {
		t.Fatalf("worst-case reply is %d bytes, want %d", len(payload), want)
	}
	reply, err := DecodeReply(payload)
	if err != nil {
		t.Fatalf("DecodeReply of a full worst-case reply: %v", err)
	}
	assertProvidersEqual(t, reply.Providers, providers)
}

func TestCapProviders(t *testing.T) {
	// More providers than fit: truncated to the cap
	many := testProviders(t, MaxProvidersPerReply+4)
	if got := capProviders(many); len(got) != MaxProvidersPerReply {
		t.Errorf("capProviders kept %d providers, want %d", len(got), MaxProvidersPerReply)
	}

	// Oversized and interleaved addrs: the first MaxAddrsPerProvider
	// fitting ones survive, longer ones are skipped
	_, pid := newIdentity(t)
	long := mustAddr(t, "/dns4/"+strings.Repeat("z", 80)+".example.com/tcp/4001")
	a1 := mustAddr(t, "/ip4/10.0.0.1/tcp/4001")
	a2 := mustAddr(t, "/ip6/2001:db8::1/udp/4001/quic-v1")
	a3 := mustAddr(t, "/ip4/10.0.0.2/tcp/4002")
	got := capProviders([]peer.AddrInfo{{ID: pid, Addrs: []ma.Multiaddr{long, a1, long, a2, a3}}})
	if len(got) != 1 {
		t.Fatalf("capProviders dropped the provider")
	}
	assertProvidersEqual(t, got, []peer.AddrInfo{{ID: pid, Addrs: []ma.Multiaddr{a1, a2}}})

	// An unrepresentable peer ID drops the entry
	bad := peer.AddrInfo{ID: peer.ID(strings.Repeat("x", MaxPeerIDLen+1))}
	if got := capProviders([]peer.AddrInfo{bad}); len(got) != 0 {
		t.Errorf("capProviders kept an oversized peer id")
	}

	// Output of capProviders always encodes
	if _, err := EncodeReply(ReplyStatusOK, capProviders(many)); err != nil {
		t.Errorf("EncodeReply rejected capped providers: %v", err)
	}
}
