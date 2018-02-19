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

const (
	PING       = 1
	PONG       = 2
	FIND_NODE  = 3
	FOUND_NODE = 4
	DIRECT     = 5
	BROADCAST  = 6
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Connect(port int) (*server, error) {
	id := generateID()
	s := &server{
		ID:         id,
		initPort:   uint16(port),
		packets:    make(chan packet, 16),
		broadcasts: make(chan BroadcastMessage, 16),
		directs:    make(chan DirectMessage, 16),
		buckets:    newBuckets(id),
	}
	err := s.connect()
	return s, err
}

type server struct {
	ID         NodeID
	initPort   uint16
	conn       net.PacketConn
	packets    chan packet
	broadcasts chan BroadcastMessage
	directs    chan DirectMessage
	buckets    *buckets
	root       bool
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

func (s server) Addr() string {
	return s.conn.LocalAddr().String()
}

func (s server) Port() int {
	addr := s.conn.LocalAddr().String()
	portStr := strings.Split(addr, ":")[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Println(err)
	}
	return port
}

func (s server) Listen() (<-chan DirectMessage, <-chan BroadcastMessage) {
	if !s.root {
		go func() {
			for s.buckets.count == 0 {
				s.ping(resolveAddr(s.initPort))
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}

	go s.startRandomLookup()
	go s.startClosestLookup()

	go func() {
		for {
			buf := make([]byte, 1024)
			n, addr, err := s.conn.ReadFrom(buf)
			if err != nil {
				log.Println(err)
				continue
			}
			packet, err := parsePacket(addr, buf[:n])
			if err != nil {
				log.Println(err)
				continue
			}
			s.packets <- packet
		}
	}()

	return s.directs, s.broadcasts
}

func (s server) startRandomLookup() {
	for {
		time.Sleep(time.Second * 2)
		randID := generateID()
		s.buckets.ExecBestNodes(randID, func(peers []bucketPeer) {
			for _, peer := range peers {
				s.findNode(peer.addr, randID)
			}
		})
	}
}

func (s server) startClosestLookup() {
	for {
		time.Sleep(time.Second * 1)
		s.buckets.ExecBestNodes(s.ID, func(peers []bucketPeer) {
			for _, peer := range peers {
				s.findNode(peer.addr, s.ID)
			}
		})
	}
}

func (s server) Broadcast(data []byte) {
	s.broadcast(32, data)
}

func (s server) broadcast(distance byte, data []byte) {
	for d := distance; d > 0; d-- {
		distance := d
		s.buckets.ExecByDistance(distance, func(peer bucketPeer) {
			s.sendBroadcast(peer.addr, byte(distance)-1, data)
		})
	}
}

func (s server) Direct(id NodeID, data []byte) {
	s.buckets.Exec(id, func(peer bucketPeer) {
		s.sendDirect(peer.addr, data)
	})
}

func (s server) DirectToFurthest(data []byte) {
	for distance := byte(1); distance <= 32; distance++ {
		s.buckets.ExecByDistance(distance, func(peer bucketPeer) {
			s.sendDirect(peer.addr, data)
		})
	}
}

func (s server) demultiplexPackets() {
	for packet := range s.packets {
		s.buckets.Add(packet.id, packet.addr)

		switch packet.header {
		case PING:
			log.Printf("Got PING from %v\n", packet.addr)
			s.buckets.Exec(packet.id, func(peer bucketPeer) {
				s.pong(packet)
			})
		case PONG:
			log.Printf("Got PONG from %v\n", packet.addr)
			s.buckets.Exec(packet.id, func(peer bucketPeer) {
				s.findNode(packet.addr, s.ID)
			})
		case FIND_NODE:
			// log.Printf("Got FIND_NODE from %v\n", packet.addr)
			searchedID := parseNodeID(packet.data[:4])
			s.buckets.ExecBestNodes(searchedID, func(peers []bucketPeer) {
				s.foundNode(packet, peers)
			})
		case FOUND_NODE:
			// log.Printf("Got FOUND_NODE from %v\n", packet.addr)
			peers := parseFound(packet.data)
			for _, peer := range peers {
				s.buckets.Add(peer.id, peer.addr)
			}
		case DIRECT:
			// log.Printf("Got DIRECT from %v\n", packet.addr)
			s.directs <- DirectMessage{
				ID:   packet.id,
				Addr: packet.addr.String(),
				Data: packet.data,
			}
		case BROADCAST:
			// log.Printf("Got BROADCAST from %v\n", packet.addr)
			distance := packet.data[0]
			data := packet.data[1:]
			s.broadcasts <- BroadcastMessage{
				ID:   packet.id,
				Addr: packet.addr.String(),
				Data: data,
				Next: func() {
					go s.broadcast(distance, data)
				},
			}
		default:
			log.Println("Got unknown header:", packet.header)
		}
	}
}

func (s server) send(addr net.Addr, buf []byte) error {
	_, err := s.conn.WriteTo(buf, addr)
	return err
}

func (s server) request(addr net.Addr, header byte, data []byte) error {
	buf := make([]byte, 9+len(data))
	buf[0] = header
	reqID := uint32(rand.Int63())
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	binary.LittleEndian.PutUint32(buf[5:], reqID)
	if len(data) > 0 {
		copy(buf[9:], data)
	}
	return s.send(addr, buf)
}

func (s server) response(packet packet, header byte, data []byte) error {
	buf := make([]byte, 9+len(data))
	buf[0] = header
	binary.LittleEndian.PutUint32(buf[1:5], uint32(s.ID))
	copy(buf[5:], packet.reqID)
	if len(data) > 0 {
		copy(buf[9:], data)
	}
	return s.send(packet.addr, buf)
}

func (s server) ping(addr net.Addr) error {
	return s.request(addr, PING, nil)
}

func (s server) pong(packet packet) error {
	return s.response(packet, PONG, nil)
}

func (s server) findNode(addr net.Addr, id NodeID) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(id))
	return s.request(addr, FIND_NODE, buf)
}

func (s server) foundNode(packet packet, best []bucketPeer) error {
	l := len(best) * 6
	buf := make([]byte, l)

	i := 0
	for _, peer := range best {
		binary.LittleEndian.PutUint32(buf[i:i+4], uint32(peer.id))
		i += 4
		port := getPort(peer.addr.String())
		binary.LittleEndian.PutUint16(buf[i:i+2], port)
		i += 2
	}

	return s.response(packet, FOUND_NODE, buf)
}

func (s server) sendBroadcast(addr net.Addr, distance byte, data []byte) error {
	buf := make([]byte, len(data)+1)
	buf[0] = distance
	copy(buf[1:], data)
	return s.request(addr, BROADCAST, buf)
}

func (s server) sendDirect(addr net.Addr, data []byte) error {
	return s.request(addr, DIRECT, data)
}

func parsePacket(addr net.Addr, buf []byte) (packet, error) {
	if len(buf) < 9 {
		return packet{}, fmt.Errorf("Packet from %v is to short, only %v bytes", addr, len(buf))
	}

	return packet{
		addr:   addr,
		header: buf[0],
		id:     parseNodeID(buf[1:5]),
		reqID:  buf[5:9],
		data:   buf[9:],
	}, nil
}

type packet struct {
	addr   net.Addr
	header byte
	id     NodeID
	reqID  []byte
	data   []byte
}

type BroadcastMessage struct {
	ID   NodeID
	Addr string
	Data []byte
	Next func()
}

type DirectMessage struct {
	ID   NodeID
	Addr string
	Data []byte
}

type found struct {
	id   NodeID
	addr net.Addr
}

func parseNodeID(buf []byte) NodeID {
	id := binary.LittleEndian.Uint32(buf)
	return NodeID(id)
}

func parseFound(buf []byte) []found {
	count := len(buf) / 6
	peers := make([]found, count, count)
	for i := 0; i < count; i++ {
		shift := i * 6
		id := binary.LittleEndian.Uint32(buf[shift : shift+4])
		port := binary.LittleEndian.Uint16(buf[shift+4 : shift+6])
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
