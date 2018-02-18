package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

func NewBlockchain() *Blockchain {
	genesis := message{}
	genesis.Hash = genesis.calcHash()
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
		Text:     text,
		Index:    last.Index + 1,
		PrevHash: last.Hash,
	}
	m.Hash = m.calcHash()
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
	if bytes.Compare(bc.blocks[0].Hash, blocks[0].Hash) != 0 {
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
	Index    uint32
	PrevHash []byte
	Hash     []byte
	Text     string
}

func (m message) validate(prev message) bool {
	if m.Index != prev.Index+1 {
		return false
	}
	if bytes.Compare(m.PrevHash, prev.Hash) != 0 {
		return false
	}
	hash := m.calcHash()
	if bytes.Compare(hash, m.Hash) != 0 {
		return false
	}
	return true
}

func (m *message) ToBytes() []byte {
	size := 4 + 32 + 32 + len(m.Text)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf, m.Index)
	copy(buf[4:], m.PrevHash)
	copy(buf[36:], m.Hash)
	copy(buf[68:], m.Text)
	return buf
}

func FromBytes(buf []byte) (*message, error) {
	if len(buf) < 68 {
		return nil, fmt.Errorf("Invalid broadcast message")
	}
	m := &message{
		Index:    binary.LittleEndian.Uint32(buf),
		PrevHash: buf[4:36],
		Hash:     buf[36:68],
		Text:     string(buf[68:]),
	}
	return m, nil
}

func (m message) calcHash() []byte {
	h := sha256.New()
	binary.Write(h, binary.LittleEndian, m.Index)
	h.Write(m.PrevHash)
	io.WriteString(h, m.Text)
	return h.Sum(nil)
}
