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
	blockchain = NewBlockchain()
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
		m := blockchain.NewMessage(fmt.Sprintf("Hi from %v", conn.conn.LocalAddr()))
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
		if blockchain.AddLast(*m) {
			fmt.Println("valid")
			broadcast.Resend()
		} else {
			fmt.Println("invalid")
			go loadBlockchainTCP(broadcast.Addr)
		}
	}
}

func handleBlockchainTCP(conn net.Conn) {
	defer conn.Close()
	encoder := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	err := encoder.Encode(blockchain.blocks)
	if err != nil {
		log.Println(err)
	}
}

func loadBlockchainTCP(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	decoder := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	var blocks []message
	err = decoder.Decode(&blocks)
	if err != nil {
		log.Println(err)
		return
	}
	if blockchain.Replace(blocks) {
		fmt.Println("Blockchain replaced:", blockchain.Last().index)
	} else {
		fmt.Println("Blockchain not replaced:", blockchain.Last().index)
	}
}

func NewBlockchain() *Blockchain {
	genesis := message{}
	genesis.hash = calcHash(genesis)
	bc := &Blockchain{
		genesis: genesis,
		blocks:  []message{genesis},
	}
	return bc
}

type Blockchain struct {
	genesis message
	blocks  []message
	mutex   sync.RWMutex
}

func (bc *Blockchain) Last() message {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	return bc.last()
}

func (bc *Blockchain) NewMessage(text string) message {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	last := bc.last()
	m := message{
		text:     text,
		index:    last.index + 1,
		prevHash: last.hash,
	}
	m.hash = calcHash(m)
	bc.blocks = append(bc.blocks, m)
	return m
}

func (bc *Blockchain) AddLast(m message) bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	last := bc.last()
	if m.validate(last) {
		bc.blocks = append(bc.blocks, m)
		return true
	}
	return false
}

func (bc *Blockchain) Replace(blocks []message) bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.validate(blocks) {
		bc.blocks = blocks
		return true
	}
	return false
}

func (bc Blockchain) validate(blocks []message) bool {
	if len(bc.blocks) >= len(blocks) {
		return false
	}
	if bytes.Compare(bc.blocks[0].hash, blocks[0].hash) != 0 {
		return false
	}
	prev := blocks[0]
	for _, m := range blocks[1:] {
		if !m.validate(prev) {
			return false
		}
		prev = m
	}
	return true
}

func (bc Blockchain) last() message {
	return bc.blocks[len(bc.blocks)-1]
}

type message struct {
	index    uint32
	prevHash []byte
	hash     []byte
	text     string
}

func (m message) validate(prev message) bool {
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

func (m *message) ToBytes() []byte {
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

func calcHash(m message) []byte {
	h := sha256.New()
	binary.Write(h, binary.LittleEndian, m.index)
	h.Write(m.prevHash)
	io.WriteString(h, m.text)
	return h.Sum(nil)
}
