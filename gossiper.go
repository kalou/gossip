package gossip

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/kalou/bloom"
)

type GossiperConfig struct {
	Address  string        // this node addr
	Region   string        // this node region
	Interval time.Duration // heartbeat itvl
	Fanout   int           // nr of nodes picked at gossips

	OnNodeJoin func(*Node) // receives node
	OnNodeDown func(*Node) // receives node
	OnNodeUp   func(*Node) // receives node

	Seed string // where to seed
	// this node from
}

type Gossiper struct {
	this *Node

	config GossiperConfig

	leaving bool

	nLock sync.RWMutex
	nodes map[string]*Node

	sLock      sync.RWMutex
	suspicious map[string]*bloom.Filter

	transport *TransportService
}

func SetPort(addr string) string {
	if addr != "" {
		_, _, perr := net.SplitHostPort(addr)
		if perr != nil {
			// Try adding default port
			return net.JoinHostPort(addr, "3000")
		}
	}

	return addr
}

func NewGossiper(config GossiperConfig) (g *Gossiper, err error) {
	if config.Address == "" {
		config.Address, err = LocalAddress()
		if err != nil {
			return
		}
	}

	config.Address = SetPort(config.Address)
	config.Seed = SetPort(config.Seed)

	g = &Gossiper{
		this:   NewNode(config.Address, config.Region),
		config: config,
	}

	if g.config.Interval == 0 {
		g.config.Interval = time.Duration(500) * time.Millisecond
	}
	if g.config.Fanout == 0 {
		g.config.Fanout = 3
	}

	g.nodes = make(map[string]*Node)
	g.suspicious = make(map[string]*bloom.Filter)

	g.transport = &TransportService{Node: g.this, OnHello: g.OnHello}

	return
}

func (g *Gossiper) GetNode(addr string) (n *Node) {
	g.nLock.Lock()
	n, _ = g.nodes[addr]
	g.nLock.Unlock()

	return n
}

func (g *Gossiper) AddNode(n *Node) {
	g.nLock.Lock()
	g.nodes[n.address] = n
	g.nLock.Unlock()
}

func (g *Gossiper) RemoveNode(n *Node) {
	g.nLock.Lock()
	delete(g.nodes, n.address)
	g.nLock.Unlock()
}

// Start the actual processes
func (g *Gossiper) Start() error {
	err := g.transport.StartServer()
	if err != nil {
		return err
	}

	fnv := fnv.New64a()
	fnv.Write([]byte(g.config.Address))
	h := fnv.Sum64()
	rand.Seed(int64(h))

	g.this.SetState(State_UP)

	g.AddNode(g.this)

	go g.Monitor()
	go g.Gossiper()
	return nil
}

// Stop and wait for completion
func (g *Gossiper) Stop() {
	g.leaving = true
}

func (g *Gossiper) Left() bool {
	return g.leaving
}

func Shuffle(xs []*Node) {
	for i := range xs {
		j := rand.Intn(i + 1)
		xs[i], xs[j] = xs[j], xs[i]
	}
}

func (g *Gossiper) Picknodes(from []*Node, cnt int) []*Node {
	// Pick up to g.config.Fanout nodes
	// If not enough nodes and ask for seed, use that
	var newfrom []*Node

	// Remove this node from list
	for _, node := range from {
		if !g.this.Equals(node) {
			newfrom = append(newfrom, node)
		}
	}

	// Randomize and..
	Shuffle(newfrom)

	if len(newfrom) < cnt {
		cnt = len(newfrom)
	}

	// Return head
	return newfrom[:cnt]
}

func (g *Gossiper) Monitor() {
	for {
		cluster := g.Status()

		for _, node := range cluster.UpNodes {
			// Ignore myself
			if g.this.Equals(node) {
				continue
			}

			timeout := time.Duration(2 * math.Max(1, float64(len(cluster.UpNodes))/float64(g.config.Fanout)) *
				(float64(g.config.Interval)))

			if node.LastSeen() > timeout {
				// Reset counter and mark suspicious
				g.sLock.Lock()
				g.suspicious[node.address] = bloom.NewFilter(len(cluster.UpNodes), .01)
				node.SetState(State_SUSPECT)
				g.sLock.Unlock()
				log.Println(node, "marked suspicious for expected timeout", timeout)
			}
		}

		time.Sleep(time.Duration(5) * time.Second)

		if g.Left() {
			return
		}
	}
}

// The emitting part just sends packets. No acks.
func (g *Gossiper) Gossiper() {
	log.Println("Starting gossiper service on", g.this.address)
	for {
		g.this.MarkSeen()
		until := time.Now().Add(g.config.Interval)

		neighbors := g.Picknodes(g.Nodes(0), g.config.Fanout)
		if len(neighbors) == 0 && g.config.Seed != "" {
			neighbors = append(neighbors, NewNode(g.config.Seed, ""))
		}

		// Not picking the same nodes speeds up contamination
		for _, node := range neighbors {
			err := g.transport.Hello(node, g.Picknodes(g.Nodes(0), g.config.Fanout))
			if err != nil {
				log.Println(node, err)
			}
		}

		time.Sleep(until.Sub(time.Now()))
		if g.Left() {
			return
		}
	}
}

// Update node table if needed and return
// the known object or nil if this is us
func (g *Gossiper) CheckNode(n *Node) *Node {
	ex := g.GetNode(n.address)
	if ex == nil && !g.this.Equals(n) && n.state == State_UP {
		g.AddNode(n)
		if g.config.OnNodeJoin != nil {
			g.config.OnNodeJoin(n)
		}
	}

	return g.GetNode(n.address)
}

func (g *Gossiper) Down(node *Node) {
	node.SetState(State_DOWN)
	// Only possible previous state is SUSPECT, right
	if g.config.OnNodeDown != nil {
		g.config.OnNodeDown(node)
	}
	log.Println(node, "down")
}

func (g *Gossiper) HandleState(node *Node, newstate State) {
	g.sLock.Lock()
	defer g.sLock.Unlock()

	if newstate == State_UP {
		switch node.state {
		case State_DOWN:
			if g.config.OnNodeUp != nil {
				g.config.OnNodeUp(node)
			}
		case 0:
			// Special case for 0 state node
			log.Println(node, "Was 0 state")
			if g.config.OnNodeJoin != nil {
				g.config.OnNodeJoin(node)
			}
		case State_UP:
			// Once they intended to leave, they have to
			// finish the leave process before they can join
			// again. Ignore that state.
			return
		case State_SUSPECT:
			// Just a normal recovery, reset the suspicion
			// counter
			delete(g.suspicious, node.address)
		default:
			log.Fatal("recover", node, newstate)
		}
	} else {
		// All other states, DOWN, SUSPECT - cannot be sent
		// by the node as their known state.
		log.Println(node, "self-sent state", newstate)
	}

	node.SetState(newstate)
}

// On Hello from node, copy table//check status//add nodes
func (g *Gossiper) OnHello(n *Node, table []*Node) {
	// First work on the talking node and update
	// its status accordingly
	n.MarkSeen()
	node := g.CheckNode(n)

	if node == nil {
		log.Fatal("Node", n, "sent hello from bad state")
	}

	g.HandleState(node, n.state)

	// Update last seen interval
	node.MarkSeen()

	for _, remote := range table {
		local := g.CheckNode(remote)
		if local == nil {
			// Node unknown but state does not
			// allow adding it. Or, it's us.
			continue
		}

		if local.LastSeen() > remote.LastSeen() {
			local.AdjustSeen(remote.LastSeen())
		}

		// Consensus on node failure if we both
		// suspect this node
		g.sLock.Lock()
		if local.state == State_SUSPECT {
			cluster := g.Status()
			switch remote.state {
			case State_SUSPECT, State_DOWN:
				//				log.Println("Confirming", remote, local)
				g.suspicious[local.address].Set(node.address) // Mark reporter
				if g.suspicious[local.address].EstimateN() >= uint(len(cluster.UpNodes)/2) {
					g.Down(local)
				}
			}
		}
		g.sLock.Unlock()

	}
}

// Returns all nodes matching state
func (g *Gossiper) Nodes(s State) (nl []*Node) {
	g.nLock.Lock()
	defer g.nLock.Unlock()

	for _, node := range g.nodes {
		if s == 0 || node.state == s {
			nl = append(nl, node)
		}
	}

	return
}

// Return the average propagation time
func average(l []*Node) (tot time.Duration) {
	if len(l) == 0 {
		return
	}

	for _, node := range l {
		tot += node.LastSeen()
	}
	tot /= time.Duration(len(l))

	return
}

func filter(l []*Node, s State) (nl []*Node) {
	for _, node := range l {
		if node.state == s {
			nl = append(nl, node)
		}
	}

	return
}

type GossiperStatus struct {
	UpNodes      []*Node
	SuspectNodes []*Node
	DownNodes    []*Node
	GossipTime   time.Duration
}

func NewStatus(neigh []*Node) GossiperStatus {
	return GossiperStatus{
		UpNodes:      filter(neigh, State_UP),
		SuspectNodes: filter(neigh, State_SUSPECT),
		DownNodes:    filter(neigh, State_DOWN),
		GossipTime:   average(filter(neigh, State_UP)),
	}
}

func regions(nl []*Node) (ret []string) {
Nodes:
	for _, node := range nl {
		for _, reg := range ret {
			if reg == node.region {
				continue Nodes
			}
		}
		ret = append(ret, node.region)
	}

	return
}

func by_region(nl []*Node, region string) (ret []*Node) {
	for _, node := range nl {
		if node.region == region {
			ret = append(ret, node)
		}
	}

	return
}

func (g *Gossiper) Status() GossiperStatus {
	return NewStatus(g.Nodes(0))
}

func (s GossiperStatus) String() string {
	return fmt.Sprintf("Up:%d Sus:%d Down:%d Avg:%s",
		len(s.UpNodes), len(s.SuspectNodes), len(s.DownNodes), s.GossipTime)
}
