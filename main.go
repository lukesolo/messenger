package main

import (
	"fmt"
	"log"
	"net"
	"strings"
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
	port := conn.Port()
	idBits := fmt.Sprintf("%b", conn.ID)
	idBits = strings.Repeat("0", 32-len(idBits)) + idBits
	log.Printf("Started to listen on port %v with NodeID\n%s\n", port, idBits)

	tcp, err := net.Listen("tcp", conn.Addr())
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
		m := blockchain.NewMessage(fmt.Sprintf("Hi from %v", conn.Port()))
		conn.Broadcast(m.ToBytes())
	}()

	go func() {
		time.Sleep(time.Second * 7)
		conn.DirectToFurthest([]byte("My direct message"))
	}()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			last := blockchain.Last()
			log.Printf("Blockchain length is %d and last message is \"%s\"\n", last.Index, last.Text)
		}
	}()

	directs, broadcasts := conn.Listen()
	for {
		select {
		case direct := <-directs:
			go handleDirect(direct)
		case broadcast := <-broadcasts:
			go handleBroadcast(broadcast)
		}
	}
}

func handleDirect(direct DirectMessage) {
	log.Println("Direct", string(direct.Data))
}

func handleBroadcast(broadcast BroadcastMessage) {
	m, err := FromBytes(broadcast.Data)
	if err != nil {
		log.Println(err)
		return
	}
	if blockchain.AddLast(*m) {
		log.Printf("Valid message with index %d from %v\n", m.Index, broadcast.Addr)
		broadcast.Resend()
	} else {
		log.Printf("Invalid message with index %d from %v\n", m.Index, broadcast.Addr)
		go sendBlockchainTCP(broadcast.Addr)
	}
}

func handleBlockchainTCP(conn net.Conn) {
	defer conn.Close()

	decoder := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	var blocks []message
	err := decoder.Decode(&blocks)
	if err != nil {
		log.Println(err)
		return
	}

	last := blockchain.Last()
	if blockchain.Replace(blocks) {
		log.Printf("Blockchain with length %d is replaced with %d blocks long\n", last.Index, len(blocks)-1)
	} else {
		log.Printf("Blockchain with length %d is not replaced with %d blocks long\n", last.Index, len(blocks)-1)
	}
}

func sendBlockchainTCP(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	encoder := codec.NewEncoder(conn, &codec.MsgpackHandle{})
	err = encoder.Encode(blockchain.blocks)
	if err != nil {
		log.Println(err)
	}
}
