package sphinx

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestModeString(t *testing.T) {
	cases := map[Mode]string{
		ModeAuto:   "auto",
		ModeRelay:  "relay",
		ModeClient: "client",
		Mode(9):    "invalid",
	}
	for m, want := range cases {
		if got := m.String(); got != want {
			t.Errorf("Mode(%d).String() = %q, want %q", int(m), got, want)
		}
	}
}

func TestKadServerMountedExactMatch(t *testing.T) {
	h := newTestHost(t)
	if kadServerMounted(h, DefaultKadServerProtocols) {
		t.Error("fresh host reports a kad server protocol")
	}

	// Kubo's dual DHT mounts the LAN server protocol on every node; a
	// suffix match on /kad/1.0.0 would promote NATed home nodes
	h.SetStreamHandler("/ipfs/lan/kad/1.0.0", func(s network.Stream) { _ = s.Reset() })
	if kadServerMounted(h, DefaultKadServerProtocols) {
		t.Error("LAN kad protocol matched the default set")
	}

	h.SetStreamHandler("/ipfs/kad/1.0.0", func(s network.Stream) { _ = s.Reset() })
	if !kadServerMounted(h, DefaultKadServerProtocols) {
		t.Error("wan kad protocol not matched")
	}

	custom := []protocol.ID{"/myfork/kad/1.0.0"}
	if kadServerMounted(h, custom) {
		t.Error("custom set matched the stock protocols")
	}
	h.SetStreamHandler("/myfork/kad/1.0.0", func(s network.Stream) { _ = s.Reset() })
	if !kadServerMounted(h, custom) {
		t.Error("custom protocol not matched by its set")
	}
}
