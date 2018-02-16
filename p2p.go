package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

const k = 5

const (
	PING       = 1
	PONG       = 2
	FIND_NODE  = 3
	FOUND_NODE = 4
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Connect(port int) (*server, error) {
	id := uint32(rand.Int63())
	s := &server{
		ID:       NodeID(id),
		initPort: uint16(port),
		packets:  make(chan packet),
	}
	err := s.connect()
	return s, err
}

type server struct {
	ID       NodeID
	initPort uint16
	conn     net.PacketConn
	packets  chan packet
	root     bool
}

func (s *server) connect() error {
	conn, err := net.ListenPacket("udp", formatAddr(s.initPort))
	if err != nil {
		log.Printf("Initial port is busy")
		conn, err = net.ListenPacket("udp", formatAddr(0))
		if err != nil {
			return err
		}
	} else {
		s.root = true
	}
	s.conn = conn

	go s.demultiplexPackets()

	return nil
}

func (s *server) Listen() {
	if !s.root {
		s.ping(resolveAddr(s.initPort))
	}

	for {
		buf := make([]byte, 1024)
		n, addr, err := s.conn.ReadFrom(buf)
		if err != nil {
			log.Println(err)
			continue
		}
		s.packets <- newPacket(addr, buf[:n])
	}
}

func (s *server) demultiplexPackets() {
	buckets := newBuckets(s.ID)

	for packet := range s.packets {
		switch packet.header {
		case PING:
			log.Printf("Got PING from %v\n", packet.addr)
			buckets.Add(packet.id, packet.addr).
				Exec(packet.id, func(peer bucketPeer) {
					s.pong(peer.addr, packet.buf)
				})
		case PONG:
			log.Printf("Got PONG from %v\n", packet.addr)
			buckets.Add(packet.id, packet.addr).
				Exec(packet.id, func(peer bucketPeer) {
					s.findNode(packet.addr)
				})
		case FIND_NODE:
			log.Printf("Got FIND_NODE from %v\n", packet.addr)
			buckets.Add(packet.id, packet.addr).
				ExecBestNodes(packet.id, func(peers []bucketPeer) {
					s.foundNode(packet.addr, packet.buf, peers)
				})
		case FOUND_NODE:
			log.Printf("Got FOUND_NODE from %v\n", packet.addr)
			peers := parseFound(packet.buf)
			for _, peer := range peers {
				buckets.Add(peer.id, peer.addr)
			}
			buckets.Print()
		}
	}
}

/*
func (s *server) demultiplexPackets() {
	m := make(map[uint32]Peer)
	buckets := make([][]Peer, 33, 33)

	addPeer := func(id uint32, addr net.Addr) Peer {
		if id == s.ID {
			return Peer{}
		}

		distance := checkDistance(s.ID, id)
		peer := Peer{id, addr, *s, distance}
		m[id] = peer
		buckets[distance] = append(buckets[distance], peer)
		return peer
	}

	printBucket := func() {
		for i, bucket := range buckets {
			if len(bucket) > 0 {
				fmt.Println(i, len(bucket))
			}
		}
	}

	for packet := range s.packets {
		distance := checkDistance(s.ID, packet.id)

		peer, ok := m[packet.id]
		if !ok {
			peer = addPeer(packet.id, packet.addr)
		}

		switch packet.header {
		case PING:
			log.Printf("Got PING from %v\n", packet.addr)
			go peer.pong(packet.buf)
		case PONG:
			log.Printf("Got PONG from %v\n", packet.addr)
			go s.findNode(packet.addr)
		case FIND_NODE:
			log.Printf("Got FIND_NODE from %v\n", packet.addr)
			best := findBestNode(buckets, distance)
			go peer.foundNode(packet.buf, best)
		case FOUND_NODE:
			log.Printf("Got FOUND_NODE from %v\n", packet.addr)
			peers := parseFound(packet.buf)
			for _, peer := range peers {
				addPeer(peer.id, peer.addr)
			}
			printBucket()
		}
	}
}
*/

func (s server) ping(addr net.Addr) error {
	buf := make([]byte, 9)
	buf[0] = PING
	reqID := uint32(rand.Int63())
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	binary.LittleEndian.PutUint32(buf[5:], reqID)
	_, err := s.conn.WriteTo(buf, addr)
	return err
}

func (s server) pong(addr net.Addr, reqId []byte) {
	buf := make([]byte, 9)
	buf[0] = PONG
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	copy(buf[5:], reqId)
	s.conn.WriteTo(buf, addr)
}

func (s server) findNode(addr net.Addr) error {
	buf := make([]byte, 9)
	buf[0] = FIND_NODE
	reqID := uint32(rand.Int63())
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	binary.LittleEndian.PutUint32(buf[5:], reqID)
	_, err := s.conn.WriteTo(buf, addr)
	return err
}

func (s server) foundNode(addr net.Addr, reqId []byte, best []bucketPeer) {
	l := 9 + len(best)*6
	buf := make([]byte, l)
	buf[0] = FOUND_NODE
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	copy(buf[5:9], reqId)

	i := 9
	for _, peer := range best {
		binary.LittleEndian.PutUint32(buf[i:i+4], uint32(peer.id))
		i += 4
		port := getPort(peer.addr.String())
		binary.LittleEndian.PutUint16(buf[i:i+2], port)
		i += 2
	}

	s.conn.WriteTo(buf, addr)
}

/*
func findBestNode(buckets [][]Peer, distance int) []Peer {
	var best []Peer
	for i := distance; i < 33; i++ {
		best = append(best, buckets[i]...)
	}
	take := k
	if len(best) < k {
		take = len(best)
	}
	return best[:take]
}

func checkDistance(id1, id2 uint32) int {
	xor := id1 ^ id2
	// fmt.Printf("%b\n%b\n%b\n", id1, id2, xor)
	return 32 - bits.LeadingZeros32(xor)
}
*/

func newPacket(addr net.Addr, buf []byte) packet {
	header := buf[0]
	id := binary.LittleEndian.Uint32(buf[1:5])
	return packet{addr, header, NodeID(id), buf[5:]}
}

type packet struct {
	addr   net.Addr
	header byte
	id     NodeID
	buf    []byte
}

type found struct {
	id   NodeID
	addr net.Addr
}

func parseFound(buf []byte) []found {
	rest := buf[4:]
	count := len(rest) / 6
	peers := make([]found, count, count)
	for i := 0; i < count; i++ {
		shift := i * 6
		id := binary.LittleEndian.Uint32(rest[shift : shift+4])
		port := binary.LittleEndian.Uint16(rest[shift+4 : shift+6])
		peers[i] = found{NodeID(id), resolveAddr(port)}
	}
	return peers
}

/*
type Peer struct {
	id       uint32
	addr     net.Addr
	server   server
	distance int
}

func (p Peer) pong(reqId []byte) {
	buf := make([]byte, 9)
	buf[0] = PONG
	binary.LittleEndian.PutUint32(buf[1:5], p.server.ID)
	copy(buf[5:], reqId)
	p.server.conn.WriteTo(buf, p.addr)
}

func (p Peer) foundNode(reqId []byte, best []Peer) {
	l := 9 + len(best)*6
	buf := make([]byte, l)
	buf[0] = FOUND_NODE
	binary.LittleEndian.PutUint32(buf[1:5], p.server.ID)
	copy(buf[5:9], reqId)

	i := 9
	for _, peer := range best {
		binary.LittleEndian.PutUint32(buf[i:i+4], peer.id)
		i += 4
		port := getPort(peer.addr.String())
		binary.LittleEndian.PutUint16(buf[i:i+2], port)
		i += 2
	}

	p.server.conn.WriteTo(buf, p.addr)
}
*/

func formatAddr(port uint16) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func getPort(addr string) uint16 {
	str := strings.Split(addr, ":")[1]
	port, err := strconv.Atoi(str)
	if err != nil {
		log.Println(err)
	}
	return uint16(port)
}

func resolveAddr(port uint16) net.Addr {
	addr, err := net.ResolveUDPAddr("udp", formatAddr(port))
	if err != nil {
		log.Println(err)
	}
	return addr
}
