package sphinx

import (
	"github.com/katzenpost/hpqc/nike/x25519"
	"github.com/katzenpost/hpqc/rand"
	"github.com/katzenpost/katzenpost/core/sphinx/geo"
)

// Design constants
const (
	// Hops per direction (forward = return)
	NrHops = 3
	// UserPayloadLength carries: command + CID (~40 B)
	// + ReturnPathsPerJob * SURB(NrHops) + encoding reserve
	UserPayloadLength = 2048
	// independent return paths per discovery job; redundancy against
	// relay churn
	ReturnPathsPerJob = 3
)

// Geometry returns the single network-wide Sphinx geometry.
// withSURB is false: replies travel through the SURBs carried inside
// the user payload, so katzenpost's extra per-packet SURB slot would
// only add 410 B of dead padding
func Geometry() *geo.Geometry {
	scheme := x25519.Scheme(rand.Reader)
	return geo.GeometryFromUserForwardPayloadLength(
		scheme, UserPayloadLength, false, NrHops)
}
