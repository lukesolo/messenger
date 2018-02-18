package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
)

const (
	LAST_MESSAGE = 1
)

func main() {
	os.Mkdir("./logs", 0666)
	portStr := os.Args[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalln(err)
	}

	msgr := NewMessenger(port)

	go func() {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5000)+5000))
		msgr.Publish(fmt.Sprintf("Hi from %v", msgr.peer.Port()))
	}()

	time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
	err = msgr.Start()
	if err != nil {
		log.Fatalln(err)
	}
	time.Sleep(time.Hour)
}

func NewMessenger(initPort int) *messenger {
	return &messenger{
		initPort: initPort,
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
	tcp, err := net.Listen("tcp", peer.Addr())
	if err != nil {
		return err
	}

	logFile, err := os.OpenFile(fmt.Sprintf("./logs/%v.log", port), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Println(err)
	}
	log.SetOutput(logFile)
	bcLogFile, err := os.OpenFile(fmt.Sprintf("./logs/%v.blockchain.log", port), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Println(err)
	}
	blockchain := NewBlockchain(bcLogFile)
	msgr.blockchain = blockchain

	idBits := fmt.Sprintf("%b", peer.ID)
	idBits = strings.Repeat("0", 32-len(idBits)) + idBits
	log.Printf("Started to listen on port %v with NodeID\n%s\n", port, idBits)

	go func() {
		defer tcp.Close()

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
			time.Sleep(time.Second * 2)
			peer.DirectToFurthest(msgr.directLastMessagePacket())
		}
	}()

	directs, broadcasts := peer.Listen()
	go func() {
		for direct := range directs {
			go msgr.handleDirect(direct)
		}
	}()
	go func() {
		for broadcast := range broadcasts {
			go msgr.handleBroadcast(broadcast)
		}
	}()
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
			return
		}

		if m.Index == last.Index {
			return
		}

		if msgr.blockchain.AddLast(*m) {
			log.Printf("Valid direct message with index %d from %v\n", m.Index, direct.Addr)
		} else {
			log.Printf("Invalid direct message with index %d from %v\n", m.Index, direct.Addr)
			go msgr.peer.Direct(direct.ID, msgr.directLastMessagePacket())
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
	last := msgr.blockchain.Last()
	if m.Index < last.Index {
		go msgr.sendBlockchainTCP(broadcast.Addr)
		return
	}

	if m.Index == last.Index {
		return
	}

	if msgr.blockchain.AddLast(*m) {
		log.Printf("Valid message with index %d from %v\n", m.Index, broadcast.Addr)
		broadcast.Next()
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
