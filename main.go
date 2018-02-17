package main

import (
	"fmt"
	"time"
)

func main() {
	conn, err := Connect(10000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%b\n", conn.ID)
	fmt.Println(conn.conn.LocalAddr())

	go func() {
		if !conn.root {
			return
		}
		for {
			time.Sleep(time.Second * 5)
			conn.Broadcast([]byte("Hi!"))
		}
	}()

	conn.Listen()
}
