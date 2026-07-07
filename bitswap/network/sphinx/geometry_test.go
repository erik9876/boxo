package sphinx

import "testing"

// Pins the derived sizes measured on 2026-07-03
// If this test fails after a dependency update, the wire format
// changed and the numbers are stale
func TestGeometryPinned(t *testing.T) {
	g := Geometry()
	if err := g.Validate(); err != nil {
		t.Fatalf("invalid geometry: %v", err)
	}
	if g.PacketLength != 2802 {
		t.Errorf("packet length = %d, want 2802", g.PacketLength)
	}
	if g.SURBLength != 408 {
		t.Errorf("surb length = %d, want 408", g.SURBLength)
	}
	// SURB bundle must fit the forward payload
	need := 1 + 40 + ReturnPathsPerJob*g.SURBLength
	if need > UserPayloadLength {
		t.Errorf("job needs %d B, payload is %d B", need, UserPayloadLength)
	}
}
