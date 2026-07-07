package sphinx

import (
	"fmt"

	kpsphinx "github.com/katzenpost/katzenpost/core/sphinx"
	"github.com/katzenpost/katzenpost/core/sphinx/commands"
)

// ForwardPath builds a katzenpost path for a forward packet; the terminal
// hop carries a Recipient command selecting the delivery handler
func ForwardPath(hops []KeyInfo, recipient RecipientID) ([]*kpsphinx.PathHop, error) {
	return buildPath(hops, []commands.RoutingCommand{&commands.Recipient{ID: recipient}})
}

// ReturnPath builds a katzenpost path for a SURB; the terminal hop is the
// requesting node and additionally carries a SURBReply with id
func ReturnPath(hops []KeyInfo, id SURBID) ([]*kpsphinx.PathHop, error) {
	return buildPath(hops, []commands.RoutingCommand{
		&commands.Recipient{},
		&commands.SURBReply{ID: id},
	})
}

// buildPath pairs each hop's NodeID with its Sphinx key. Intermediate hops
// carry no commands: katzenpost generates NextNodeHop itself and rejects
// explicit ones. Exactly NrHops; a shorter path would silently weaken the
// anonymity the geometry is sized for
func buildPath(hops []KeyInfo, terminal []commands.RoutingCommand) ([]*kpsphinx.PathHop, error) {
	if len(hops) != NrHops {
		return nil, fmt.Errorf("path needs exactly %d hops, got %d", NrHops, len(hops))
	}
	path := make([]*kpsphinx.PathHop, len(hops))
	for i, h := range hops {
		if h.PublicKey == nil {
			return nil, fmt.Errorf("hop %d (%s) has no sphinx public key", i, h.PeerID)
		}
		id, err := NodeIDFromPeer(h.PeerID)
		if err != nil {
			return nil, fmt.Errorf("hop %d: %w", i, err)
		}
		hop := &kpsphinx.PathHop{ID: id, NIKEPublicKey: h.PublicKey}
		if i == len(hops)-1 {
			hop.Commands = terminal
		}
		path[i] = hop
	}
	return path, nil
}
