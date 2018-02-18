package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ugorji/go/codec"
)

var blockchain *Blockchain

func init() {
	blockchain = newBlockchain()
}

func main() {
	conn, err := Connect(10000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%b\n", conn.ID)
	fmt.Println(conn.conn.LocalAddr())

	tcp, err := net.Listen("tcp", conn.conn.LocalAddr().String())
	if err != nil {
		panic(err)
	}
	defer tcp.Close()

	go func() {
		for {
			conn, err := tcp.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			go handleBlockchainTCP(conn)
		}
	}()

	go func() {
		time.Sleep(time.Second * 10)
		m := blockchain.newMessage(fmt.Sprintf("Hi from %v", conn.conn.LocalAddr()))
		conn.Broadcast(m.ToBytes())
	}()

	broadcasts := conn.Listen()
	for broadcast := range broadcasts {
		m, err := FromBytes(broadcast.Data)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(m.index, m.text)
		if blockchain.addLastMessage(m) {
			fmt.Println("valid")
			broadcast.Resend()
		}
	}
}

func handleBlockchainTCP(conn net.Conn) {
	defer conn.Close()
	encoder := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	err := encoder.Encode(blockchain.last)
	if err != nil {
		log.Println(err)
	}
}

func newBlockchain() *Blockchain {
	genesis := &message{}
	genesis.hash = calcHash(genesis)
	bc := &Blockchain{
		genesis: genesis,
		last:    genesis,
	}
	return bc
}

type Blockchain struct {
	genesis *message
	last    *message
	mutex   sync.RWMutex
}

func (bc *Blockchain) newMessage(text string) *message {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	m := &message{
		text:     text,
		index:    bc.last.index + 1,
		prevHash: bc.last.hash,
		prev:     bc.last,
	}
	m.hash = calcHash(m)
	bc.last = m
	return m
}

func (bc *Blockchain) addLastMessage(m *message) bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if m.validate(bc.last) {
		m.prev = bc.last
		bc.last = m
		return true
	}
	return false
}

type message struct {
	index    uint32
	prevHash []byte
	hash     []byte
	text     string
	prev     *message
}

func (m *message) validate(prev *message) bool {
	if m.index != prev.index+1 {
		return false
	}
	if bytes.Compare(m.prevHash, prev.hash) != 0 {
		return false
	}
	hash := calcHash(m)
	if bytes.Compare(hash, m.hash) != 0 {
		return false
	}
	return true
}

func (m message) ToBytes() []byte {
	size := 4 + 32 + 32 + len(m.text)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf, m.index)
	copy(buf[4:], m.prevHash)
	copy(buf[36:], m.hash)
	copy(buf[68:], m.text)
	return buf
}

func FromBytes(buf []byte) (*message, error) {
	if len(buf) < 68 {
		return nil, fmt.Errorf("Invalid broadcast message")
	}
	m := &message{
		index:    binary.LittleEndian.Uint32(buf),
		prevHash: buf[4:36],
		hash:     buf[36:68],
		text:     string(buf[68:]),
	}
	return m, nil
}

func calcHash(m *message) []byte {
	h := sha256.New()
	binary.Write(h, binary.LittleEndian, m.index)
	h.Write(m.prevHash)
	io.WriteString(h, m.text)
	return h.Sum(nil)
}
