package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"time"
)

var lastMessage = message{}

func init() {
	lastMessage.hash = calcHash(lastMessage)
}

func main() {
	fmt.Println(len(lastMessage.hash))
	conn, err := Connect(10000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%b\n", conn.ID)
	fmt.Println(conn.conn.LocalAddr())

	go func() {
		time.Sleep(time.Second * 10)
		m := newMessage(fmt.Sprintf("Hi from %v", conn.conn.LocalAddr()), lastMessage)
		lastMessage = m
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
		if m.validate(lastMessage) {
			fmt.Println("valid")
			lastMessage = m
			broadcast.Resend()
		}
	}
}

func newMessage(text string, last message) message {
	m := message{
		index:    last.index + 1,
		prevHash: last.hash,
		text:     text,
	}
	m.hash = calcHash(m)
	return m
}

type message struct {
	index    uint32
	prevHash []byte
	hash     []byte
	text     string
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

func FromBytes(buf []byte) (message, error) {
	if len(buf) < 68 {
		return message{}, fmt.Errorf("Invalid broadcast message")
	}
	m := message{
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
