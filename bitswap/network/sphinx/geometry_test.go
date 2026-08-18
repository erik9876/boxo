package sphinx

import "testing"

// Pins the derived sizes, re-measured on 2026-08-18 after dropping the
// katzenpost SURB slot (withSURB=false). If this test fails after a
// dependency update, the wire format changed and the numbers are stale
func TestGeometryPinned(t *testing.T) {
	g := Geometry()
	if err := g.Validate(); err != nil {
		t.Fatalf("invalid geometry: %v", err)
	}
	if g.PacketLength != 2392 {
		t.Errorf("packet length = %d, want 2392", g.PacketLength)
	}
	if g.SURBLength != 408 {
		t.Errorf("surb length = %d, want 408", g.SURBLength)
	}
	// fit checks go against the wire, not the budget constant: the
	// padding prefix puts capacity 2 B under UserPayloadLength
	capacity := g.ForwardPayloadLength - payloadLenPrefix
	if worst := jobHeaderLen + MaxCIDLen + ReturnPathsPerJob*g.SURBLength; worst > capacity {
		t.Errorf("worst-case job is %d B, wire capacity is %d B", worst, capacity)
	}
	if worst := replyHeaderLen + MaxProvidersPerReply*maxProviderEntryLen; worst > capacity {
		t.Errorf("worst-case reply is %d B, wire capacity is %d B", worst, capacity)
	}
}
