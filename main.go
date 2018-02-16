package main

import (
	"fmt"
)

func main() {
	conn, err := Connect(10000)
	if err != nil {
		panic(err)
	}
	fmt.Println(conn)
	conn.Listen()
}
