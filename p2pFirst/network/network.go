package network

import (
	"fmt"
	"net"
)

const (
	queryAll = iota
	addBlock
	response
)

func queryAllBlocks(conn net.Conn) {
	conn.Write([]byte("func:1"))

}

func handleConn(conn net.Conn) {
	defer conn.Close()

	data := make([]byte, 4096)
	n, err := conn.Read(data)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(n, string(data[:n]))

}
