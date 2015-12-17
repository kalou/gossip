package gossip

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
)

// This transport uses UDP and
// protobuf for node communication
type TransportService struct {
	Node    *Node
	OnHello func(n *Node, table []*Node)

	server *net.UDPConn // Listening/replying to UDP msg
}

// Instanciate a service for the ring
func (t *TransportService) StartServer() error {
	addr, err := net.ResolveUDPAddr("udp", t.Node.address)
	if err != nil {
		return err
	}

	t.server, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	t.server.SetReadBuffer(128 * 1024)

	go func() {
		buf := make([]byte, 4096)
		for {
			cnt, remote, err := t.server.ReadFromUDP(buf)
			if err != nil {
				log.Println("read", err)
			}

			msg := &PbMessage{}

			err = proto.Unmarshal(buf[:cnt], msg)
			if err != nil {
				log.Println("Cannot decode", cnt, "bytes in", buf, err)
				return
			}

			go t.OnMessage(remote, msg)
		}
	}()

	return nil
}

// Functions sending message to the other services
func (t *TransportService) NewMessage(mtype PbMessage_Type) *PbMessage {
	return &PbMessage{
		Type: mtype.Enum(),
		Id:   proto.Uint32(uint32(rand.Int31())),
		Src:  t.Node.ToPb(),
	}
}

func (t *TransportService) SendMsg(other *Node, msg *PbMessage) (err error) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		log.Println(err)
		return
	}

	t.server.SetWriteDeadline(time.Now().Add(time.Second))
	t.server.WriteTo(bytes, UDPAddr(other.address, false))
	t.server.SetWriteDeadline(time.Time{})
	return
}

// Outgoing message impl.
func (t *TransportService) Hello(dst *Node, nodes []*Node) (err error) {
	msg := t.NewMessage(PbMessage_HELLO)

	for _, n := range nodes {
		msg.Neighbors = append(msg.Neighbors, n.ToPb())
	}

	err = t.SendMsg(dst, msg)
	if err != nil {
		return
	}

	return
}

// Incoming message impl.
func (t *TransportService) OnMessage(peer net.Addr, msg *PbMessage) {

	switch *msg.Type {
	case PbMessage_HELLO:
		// XXX authentify source. Not only with a simple addrcheck though.
		t.OnHello(PbToNode(msg.Src), PbToNodeList(msg.Neighbors))
		return
	}

	log.Println("Got message ", msg)
}

// Helper functions
func LocalAddress() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println("Cannot get addrlist for hostname")
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			return ipnet.IP.String(), nil
		}
	}

	return "", errors.New("Didnt find suitable Ip")
}

func UDPAddr(addr string, client bool) *net.UDPAddr {
	host, sport, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatal("Should not happen or fixme: ", err, addr)
	}

	port, err := strconv.Atoi(sport)
	if err != nil {
		log.Fatal("Should not happen")
	}

	if client {
		port = 0
	}

	return &net.UDPAddr{
		IP:   net.ParseIP(host),
		Port: port,
	}
}
