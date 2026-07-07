package sphinx

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// Job and reply codec for the payloads of the packet core. Both encodings
// check against the 2048 B UserPayloadLength budget, not the ~2456 B
// padding capacity (see padPayload)

// 0x00 stays invalid so a zeroed buffer never parses as a job
const cmdDiscoverV1 byte = 0x01

type ReplyStatus byte

const (
	// provider list may be empty: "no providers found" is a valid result
	ReplyStatusOK ReplyStatus = 0x01
	// discovery itself failed; provider list empty
	ReplyStatusFailed ReplyStatus = 0x02
)

// DiscoveryRecipient addresses the discovery job handler at the exit hop.
// ASCII, zero-padded, readable in a hex dump
var DiscoveryRecipient = func() RecipientID {
	var id RecipientID
	copy(id[:], "sphinx/discover/1")
	return id
}()

// command byte + cidLen byte + SURB count byte
const jobHeaderLen = 3

// Wire caps; they bound every variable-length field so
// MaxProvidersPerReply follows from a worst case
const (
	// realistic CIDs are 34-40 B; 128 leaves room for exotic codecs
	MaxCIDLen = 128

	// identity-embedded ed25519 peer ID: 38 B, hashed (RSA etc.): 34 B
	MaxPeerIDLen = 42

	// one dialable addr suffices, the second covers a another transport;
	// the initiator learns the rest via identify
	MaxAddrsPerProvider = 2

	// 40 covers direct ip4/ip6 x tcp/udp/quic/ws. DNS, /certhash and
	// /p2p-circuit composites are unbounded and stay over the cap: such a
	// provider keeps its entry but loses those addrs
	MaxAddrLen = 40

	// worst-case provider entry: 1 + 42 + 1 + 2·(1+40) = 126 B
	maxProviderEntryLen = 1 + MaxPeerIDLen + 1 + MaxAddrsPerProvider*(1+MaxAddrLen)

	// status byte + count byte
	replyHeaderLen = 2

	// most providers guaranteed to fit the budget at worst-case entry
	// size: ⌊(2048−2)/126⌋ = 16. Lets the proxy take the first 16 results
	// without a size-fitting loop
	MaxProvidersPerReply = (UserPayloadLength - replyHeaderLen) / maxProviderEntryLen
)

// Job: find providers for CID, answer through every SURB. Wire layout
// (all lengths single bytes):
//
//	[1] command (cmdDiscoverV1)
//	[1] cidLen   [cidLen] CID (binary)
//	[1] m        [m·SURBLength] SURBs
type Job struct {
	CID   cid.Cid
	SURBs [][]byte
}

// EncodeJob encodes a job for c answered through surbs; m is bounded only
// by the payload budget
func EncodeJob(c cid.Cid, surbs [][]byte) ([]byte, error) {
	surbLen := Geometry().SURBLength
	if !c.Defined() {
		return nil, errors.New("job needs a defined cid")
	}
	cb := c.Bytes()
	if len(cb) > MaxCIDLen {
		return nil, fmt.Errorf("cid of %d bytes exceeds %d", len(cb), MaxCIDLen)
	}
	if len(surbs) == 0 {
		return nil, errors.New("job without surbs cannot be answered")
	}
	if len(surbs) > 255 {
		return nil, fmt.Errorf("%d surbs exceed the count byte", len(surbs))
	}
	for i, s := range surbs {
		if len(s) != surbLen {
			return nil, fmt.Errorf("surb %d is %d bytes, want %d", i, len(s), surbLen)
		}
	}
	total := jobHeaderLen + len(cb) + len(surbs)*surbLen
	if total > UserPayloadLength {
		return nil, fmt.Errorf("encoded job of %d bytes exceeds the %d byte budget", total, UserPayloadLength)
	}

	buf := make([]byte, 0, total)
	buf = append(buf, cmdDiscoverV1, byte(len(cb)))
	buf = append(buf, cb...)
	buf = append(buf, byte(len(surbs)))
	for _, s := range surbs {
		buf = append(buf, s...)
	}
	return buf, nil
}

// DecodeJob fully validates; any structural inconsistency rejects the
// whole payload. Returned SURBs are copies, payload may alias a packet
// buffer
func DecodeJob(payload []byte) (*Job, error) {
	surbLen := Geometry().SURBLength
	if len(payload) > UserPayloadLength {
		return nil, fmt.Errorf("job payload of %d bytes exceeds the %d byte budget", len(payload), UserPayloadLength)
	}
	if len(payload) < jobHeaderLen {
		return nil, fmt.Errorf("job payload of %d bytes is shorter than its header", len(payload))
	}
	if payload[0] != cmdDiscoverV1 {
		return nil, fmt.Errorf("unknown job command 0x%02x", payload[0])
	}
	cidLen := int(payload[1])
	if cidLen == 0 || cidLen > MaxCIDLen {
		return nil, fmt.Errorf("cid length %d outside 1..%d", cidLen, MaxCIDLen)
	}
	rest := payload[2:]
	if len(rest) < cidLen+1 {
		return nil, fmt.Errorf("job truncated inside the cid field")
	}
	c, err := cid.Cast(rest[:cidLen])
	if err != nil {
		return nil, fmt.Errorf("parsing job cid: %w", err)
	}
	m := int(rest[cidLen])
	if m == 0 {
		return nil, errors.New("job without surbs cannot be answered")
	}
	rest = rest[cidLen+1:]
	if len(rest) != m*surbLen {
		return nil, fmt.Errorf("job carries %d surb bytes, want %d for %d surbs", len(rest), m*surbLen, m)
	}
	surbs := make([][]byte, m)
	for i := range surbs {
		surbs[i] = append([]byte(nil), rest[i*surbLen:(i+1)*surbLen]...)
	}
	return &Job{CID: c, SURBs: surbs}, nil
}

// Reply wire layout (all lengths single bytes):
//
//	[1] status
//	[1] n (provider count, <= MaxProvidersPerReply)
//	n × { [1] pidLen  [pidLen] peer ID (binary)
//	      [1] aCount  aCount × { [1] addrLen  [addrLen] multiaddr } }
//
// No CID echo; the initiator maps replies to jobs by SURB ID
type Reply struct {
	Status    ReplyStatus
	Providers []peer.AddrInfo
}

// EncodeReply validates strictly against the wire caps; callers run
// discovery output through capProviders first
func EncodeReply(status ReplyStatus, providers []peer.AddrInfo) ([]byte, error) {
	switch status {
	case ReplyStatusOK:
	case ReplyStatusFailed:
		if len(providers) != 0 {
			return nil, errors.New("failed reply must not carry providers")
		}
	default:
		return nil, fmt.Errorf("unknown reply status 0x%02x", byte(status))
	}
	if len(providers) > MaxProvidersPerReply {
		return nil, fmt.Errorf("%d providers exceed MaxProvidersPerReply (%d)", len(providers), MaxProvidersPerReply)
	}

	buf := make([]byte, 0, replyHeaderLen+len(providers)*maxProviderEntryLen)
	buf = append(buf, byte(status), byte(len(providers)))
	for i, p := range providers {
		pid := []byte(p.ID)
		if len(pid) == 0 || len(pid) > MaxPeerIDLen {
			return nil, fmt.Errorf("provider %d: peer id of %d bytes outside 1..%d", i, len(pid), MaxPeerIDLen)
		}
		if len(p.Addrs) > MaxAddrsPerProvider {
			return nil, fmt.Errorf("provider %d: %d addrs exceed %d", i, len(p.Addrs), MaxAddrsPerProvider)
		}
		buf = append(buf, byte(len(pid)))
		buf = append(buf, pid...)
		buf = append(buf, byte(len(p.Addrs)))
		for j, a := range p.Addrs {
			ab := a.Bytes()
			if len(ab) == 0 || len(ab) > MaxAddrLen {
				return nil, fmt.Errorf("provider %d addr %d: %d bytes outside 1..%d", i, j, len(ab), MaxAddrLen)
			}
			buf = append(buf, byte(len(ab)))
			buf = append(buf, ab...)
		}
	}
	// guaranteed by the caps; a broken cap must not leak an overlong
	// payload into padPayload
	if len(buf) > UserPayloadLength {
		return nil, fmt.Errorf("encoded reply of %d bytes exceeds the %d byte budget", len(buf), UserPayloadLength)
	}
	return buf, nil
}

// DecodeReply fully validates; any structural inconsistency rejects the
// whole payload. Returned data is privately owned
func DecodeReply(payload []byte) (*Reply, error) {
	if len(payload) > UserPayloadLength {
		return nil, fmt.Errorf("reply payload of %d bytes exceeds the %d byte budget", len(payload), UserPayloadLength)
	}
	if len(payload) < replyHeaderLen {
		return nil, fmt.Errorf("reply payload of %d bytes is shorter than its header", len(payload))
	}
	status := ReplyStatus(payload[0])
	if status != ReplyStatusOK && status != ReplyStatusFailed {
		return nil, fmt.Errorf("unknown reply status 0x%02x", payload[0])
	}
	n := int(payload[1])
	if n > MaxProvidersPerReply {
		return nil, fmt.Errorf("%d providers exceed MaxProvidersPerReply (%d)", n, MaxProvidersPerReply)
	}
	if status == ReplyStatusFailed && n != 0 {
		return nil, errors.New("failed reply must not carry providers")
	}

	rest := payload[replyHeaderLen:]
	providers := make([]peer.AddrInfo, 0, n)
	for i := range n {
		if len(rest) < 1 {
			return nil, fmt.Errorf("reply truncated before provider %d", i)
		}
		pidLen := int(rest[0])
		if pidLen == 0 || pidLen > MaxPeerIDLen {
			return nil, fmt.Errorf("provider %d: peer id length %d outside 1..%d", i, pidLen, MaxPeerIDLen)
		}
		rest = rest[1:]
		if len(rest) < pidLen+1 {
			return nil, fmt.Errorf("reply truncated inside provider %d peer id", i)
		}
		pid, err := peer.IDFromBytes(rest[:pidLen])
		if err != nil {
			return nil, fmt.Errorf("provider %d: parsing peer id: %w", i, err)
		}
		aCount := int(rest[pidLen])
		if aCount > MaxAddrsPerProvider {
			return nil, fmt.Errorf("provider %d: %d addrs exceed %d", i, aCount, MaxAddrsPerProvider)
		}
		rest = rest[pidLen+1:]

		addrs := make([]ma.Multiaddr, 0, aCount)
		for j := range aCount {
			if len(rest) < 1 {
				return nil, fmt.Errorf("reply truncated before provider %d addr %d", i, j)
			}
			addrLen := int(rest[0])
			if addrLen == 0 || addrLen > MaxAddrLen {
				return nil, fmt.Errorf("provider %d addr %d: length %d outside 1..%d", i, j, addrLen, MaxAddrLen)
			}
			rest = rest[1:]
			if len(rest) < addrLen {
				return nil, fmt.Errorf("reply truncated inside provider %d addr %d", i, j)
			}
			a, err := ma.NewMultiaddrBytes(append([]byte(nil), rest[:addrLen]...))
			if err != nil {
				return nil, fmt.Errorf("provider %d addr %d: parsing multiaddr: %w", i, j, err)
			}
			addrs = append(addrs, a)
			rest = rest[addrLen:]
		}
		providers = append(providers, peer.AddrInfo{ID: pid, Addrs: addrs})
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("reply carries %d trailing bytes", len(rest))
	}
	return &Reply{Status: status, Providers: providers}, nil
}

// capProviders trims discovery output to what EncodeReply accepts: first
// MaxProvidersPerReply entries, per entry the first MaxAddrsPerProvider
// addrs within MaxAddrLen
func capProviders(providers []peer.AddrInfo) []peer.AddrInfo {
	out := make([]peer.AddrInfo, 0, min(len(providers), MaxProvidersPerReply))
	for _, p := range providers {
		if len(out) == MaxProvidersPerReply {
			break
		}
		if len(p.ID) == 0 || len(p.ID) > MaxPeerIDLen {
			continue
		}
		addrs := make([]ma.Multiaddr, 0, MaxAddrsPerProvider)
		for _, a := range p.Addrs {
			if len(addrs) == MaxAddrsPerProvider {
				break
			}
			if n := len(a.Bytes()); n == 0 || n > MaxAddrLen {
				continue
			}
			addrs = append(addrs, a)
		}
		out = append(out, peer.AddrInfo{ID: p.ID, Addrs: addrs})
	}
	return out
}

// DeliveryMux fans the relay's DeliveryHandler out to both roles:
// deliveries to the proxy, SURB replies to the job manager. Roles are set
// after construction (they need the transport, which needs the relay);
// setting is safe while packets arrive
type DeliveryMux struct {
	mu    sync.Mutex
	proxy *Proxy
	jobs  *JobManager
}

func (m *DeliveryMux) SetProxy(p *Proxy) {
	m.mu.Lock()
	m.proxy = p
	m.mu.Unlock()
}

func (m *DeliveryMux) SetJobs(j *JobManager) {
	m.mu.Lock()
	m.jobs = j
	m.mu.Unlock()
}

func (m *DeliveryMux) HandleDelivery(recipient RecipientID, payload []byte) {
	m.mu.Lock()
	p := m.proxy
	m.mu.Unlock()
	if p == nil {
		log.Debugw("dropping delivery: node runs no proxy role")
		return
	}
	p.HandleDelivery(recipient, payload)
}

func (m *DeliveryMux) HandleSURBReply(id SURBID, payload []byte) {
	m.mu.Lock()
	j := m.jobs
	m.mu.Unlock()
	if j == nil {
		log.Debugw("dropping surb reply: node runs no initiator role", "surb", fmt.Sprintf("%x", id))
		return
	}
	j.HandleSURBReply(id, payload)
}
