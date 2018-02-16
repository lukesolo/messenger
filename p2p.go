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
	id := generateID()
	s := &server{
		ID:       id,
		initPort: uint16(port),
		packets:  make(chan packet),
		buckets:  newBuckets(id),
	}
	err := s.connect()
	return s, err
}

type server struct {
	ID       NodeID
	initPort uint16
	conn     net.PacketConn
	packets  chan packet
	buckets  buckets
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

func (s server) Listen() {
	if !s.root {
		s.ping(resolveAddr(s.initPort))
	}

	go s.startRandomLookup()

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

func (s server) startRandomLookup() {
	for {
		time.Sleep(time.Second * 5)
		randID := generateID()
		s.buckets.ExecBestNodes(randID, func(peers []bucketPeer) {
			for _, peer := range peers {
				s.findNode(peer.addr, randID)
			}
		})
	}
}

func (s server) demultiplexPackets() {
	for packet := range s.packets {
		switch packet.header {
		case PING:
			log.Printf("Got PING from %v\n", packet.addr)
			s.buckets.Add(packet.id, packet.addr).
				Exec(packet.id, func(peer bucketPeer) {
					s.pong(peer.addr, packet.buf)
				})
		case PONG:
			log.Printf("Got PONG from %v\n", packet.addr)
			s.buckets.Add(packet.id, packet.addr).
				Exec(packet.id, func(peer bucketPeer) {
					s.findNode(packet.addr, s.ID)
				})
		case FIND_NODE:
			log.Printf("Got FIND_NODE from %v\n", packet.addr)
			s.buckets.ExecBestNodes(packet.id, func(peers []bucketPeer) {
				s.foundNode(packet.addr, packet.buf, peers)
			})
		case FOUND_NODE:
			log.Printf("Got FOUND_NODE from %v\n", packet.addr)
			peers := parseFound(packet.buf)
			for _, peer := range peers {
				s.buckets.Add(peer.id, peer.addr)
			}
			s.buckets.Print()
		}
	}
}

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

func (s server) findNode(addr net.Addr, id NodeID) error {
	buf := make([]byte, 9)
	buf[0] = FIND_NODE
	reqID := uint32(rand.Int63())
	binary.LittleEndian.PutUint32(buf[1:5], uint32(id))
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

func generateID() NodeID {
	return NodeID(rand.Int63())
}
