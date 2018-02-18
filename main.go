package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
)

const (
	LAST_MESSAGE = 1
)

func main() {
	msgr := NewMessenger(10000)

	go func() {
		time.Sleep(time.Second * 3)
		msgr.Publish(fmt.Sprintf("Hi from %v", msgr.peer.Port()))
	}()

	err := msgr.Start()
	if err != nil {
		log.Fatalln(err)
	}
}

func NewMessenger(initPort int) *messenger {
	return &messenger{
		initPort:   initPort,
		blockchain: NewBlockchain(),
	}
}

type messenger struct {
	initPort   int
	peer       *server
	blockchain *Blockchain
}

func (msgr *messenger) Start() error {
	peer, err := Connect(msgr.initPort)
	if err != nil {
		return err
	}
	msgr.peer = peer

	port := peer.Port()
	idBits := fmt.Sprintf("%b", peer.ID)
	idBits = strings.Repeat("0", 32-len(idBits)) + idBits
	log.Printf("Started to listen on port %v with NodeID\n%s\n", port, idBits)

	tcp, err := net.Listen("tcp", peer.Addr())
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
			go msgr.handleBlockchainTCP(conn)
		}
	}()

	go func() {
		for {
			time.Sleep(time.Millisecond * 500)
			peer.DirectToFurthest(msgr.directLastMessagePacket())
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			last := msgr.blockchain.Last()
			log.Printf("Blockchain length is %d and last message is \"%s\"\n", last.Index, last.Text)
		}
	}()

	directs, broadcasts := peer.Listen()
	go func() {
		for direct := range directs {
			go msgr.handleDirect(direct)
		}
	}()
	for broadcast := range broadcasts {
		go msgr.handleBroadcast(broadcast)
	}
	return nil
}

func (msgr messenger) Publish(text string) {
	m := msgr.blockchain.NewMessage(text)
	msgr.peer.Broadcast(m.ToBytes())
}

func (msgr messenger) handleDirect(direct DirectMessage) {
	switch direct.Data[0] {
	case LAST_MESSAGE:
		m, err := FromBytes(direct.Data[1:])
		if err != nil {
			log.Println(err)
			return
		}
		last := msgr.blockchain.Last()
		if m.Index < last.Index {
			go msgr.sendBlockchainTCP(direct.Addr)
		}

		if m.Index == last.Index {
			return
		}

		if !msgr.blockchain.AddLast(*m) {
			log.Printf("Invalid direct message with index %d from %v\n", m.Index, direct.Addr)
			go msgr.peer.Direct(direct.ID, msgr.directLastMessagePacket())
		} else {
			log.Printf("Valid direct message with index %d from %v\n", m.Index, direct.Addr)
		}
	default:
		log.Println("Unknown header", direct.Data[0])
	}
}

func (msgr messenger) handleBroadcast(broadcast BroadcastMessage) {
	m, err := FromBytes(broadcast.Data)
	if err != nil {
		log.Println(err)
		return
	}
	if msgr.blockchain.AddLast(*m) {
		log.Printf("Valid message with index %d from %v\n", m.Index, broadcast.Addr)
		broadcast.Resend()
	} else {
		log.Printf("Invalid message with index %d from %v\n", m.Index, broadcast.Addr)
		go msgr.sendBlockchainTCP(broadcast.Addr)
	}
}

func (msgr messenger) handleBlockchainTCP(conn net.Conn) {
	defer conn.Close()

	decoder := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	var blocks []message
	err := decoder.Decode(&blocks)
	if err != nil {
		log.Println(err)
		return
	}

	last := msgr.blockchain.Last()
	if msgr.blockchain.Replace(blocks) {
		log.Printf("Blockchain with length %d is replaced with %d blocks long\n", last.Index, len(blocks)-1)
	} else {
		log.Printf("Blockchain with length %d is not replaced with %d blocks long\n", last.Index, len(blocks)-1)
	}
}

func (msgr messenger) sendBlockchainTCP(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	encoder := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	err = encoder.Encode(msgr.blockchain.blocks)
	if err != nil {
		log.Println(err)
	}
}

func (msgr messenger) directLastMessagePacket() []byte {
	last := msgr.blockchain.Last()
	data := last.ToBytes()
	buf := make([]byte, len(data)+1)
	buf[0] = LAST_MESSAGE
	copy(buf[1:], data)
	return buf
}
