package sphinx

import (
	"slices"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// Mode gates whether a node serves its Sphinx key and is therefore
// eligible as relay/proxy in remote pools. Only DHT servers make useful
// relays; a client keeps a local key for the SURB terminal hop but never
// puts it on the wire, because nothing ever needs a client's key remotely
type Mode int

const (
	// ModeAuto follows the host's mounted kad server protocol, including
	// later promotion and demotion
	ModeAuto Mode = iota
	// ModeRelay always serves the key
	ModeRelay
	// ModeClient never serves the key
	ModeClient
)

func (m Mode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeRelay:
		return "relay"
	case ModeClient:
		return "client"
	default:
		return "invalid"
	}
}

// DefaultKadServerProtocols is what ModeAuto matches when the config
// leaves KadServerProtocols empty. Exact IDs, deliberately no suffix
// match: Kubo's dual DHT mounts /ipfs/lan/kad/1.0.0 in server mode on
// every node regardless of the WAN role, so matching on a /kad/1.0.0
// suffix would promote NATed home nodes to relays
var DefaultKadServerProtocols = []protocol.ID{"/ipfs/kad/1.0.0"}

// kadServerMounted reports whether h currently mounts one of the kad
// server protocols
func kadServerMounted(h host.Host, kadProtos []protocol.ID) bool {
	mounted := h.Mux().Protocols()
	for _, kp := range kadProtos {
		if slices.Contains(mounted, kp) {
			return true
		}
	}
	return false
}
