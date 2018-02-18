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
		for {
			time.Sleep(time.Second * 5)
			log.Printf("My last message is \"%s\"\n", blockchain.Last().Text)
		}
	}()

	broadcasts := conn.Listen()
	for broadcast := range broadcasts {
		m, err := FromBytes(broadcast.Data)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(m.Index, m.Text)
		if blockchain.AddLast(*m) {
			fmt.Println("valid from", broadcast.Addr)
			broadcast.Resend()
		} else {
			fmt.Println("invalid from", broadcast.Addr)
			go sendBlockchainTCP(broadcast.Addr)
		}
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

	fmt.Println("Received blocks:", len(blocks))
	if blockchain.Replace(blocks) {
		fmt.Println("Blockchain replaced:", blockchain.Last().Index)
	} else {
		fmt.Println("Blockchain not replaced:", blockchain.Last().Index)
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
