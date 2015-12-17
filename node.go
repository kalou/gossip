package gossip

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
)

type Node struct {
	address string
	region  string
	state   State

	lastseen time.Time

	version int32 // Node version known
}

func NewNode(address string, region string) *Node {
	if region == "" {
		region = "default"
	}

	return &Node{address: address, region: region}
}

func (n *Node) SetState(s State) State {
	old := n.state
	n.state = s

	return old
}

func (n *Node) MarkSeen() {
	n.lastseen = time.Now()
}

func (n *Node) LastSeen() time.Duration {
	return time.Since(n.lastseen)
}

func (n *Node) AdjustSeen(since time.Duration) {
	n.lastseen = time.Now().Add(-since)
}

func (n *Node) Equals(other *Node) bool {
	if n.address == other.address {
		return true
	}
	return false
}

func (n *Node) ToPb() *PbNode {
	// Note on lastseen: wire format is a number of second
	// since last seen. Nodes have a good approximation
	// after PbToNode() unmarshall. We add an extra second
	// to avoid the network maintaining a short ghost duration.
	return &PbNode{
		Address:  proto.String(n.address),
		Region:   proto.String(n.region),
		State:    n.state.Enum(),
		Lastseen: proto.Int32(int32(n.LastSeen().Seconds()) + 1),
		Version:  proto.Int32(n.version),
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("%s@%s <%s> (%s)", n.address, n.region, n.state, n.LastSeen())
}

func PbToNode(p *PbNode) *Node {
	node := NewNode(p.GetAddress(), p.GetRegion())
	node.state = p.GetState()
	node.AdjustSeen(time.Duration(p.GetLastseen()) * time.Second)
	node.version = p.GetVersion()

	return node
}

func PbToNodeList(pl []*PbNode) (nl []*Node) {
	for _, n := range pl {
		nl = append(nl, PbToNode(n))
	}
	return
}
