package main

import (
	"fmt"
)

func main() {
	conn, err := Connect(10000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%b\n", conn.ID)
	fmt.Println(conn.conn.LocalAddr())
	conn.Listen()
}
