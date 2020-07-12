package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"p2pFirst/blockchain"
	"strings"
	"time"
)

// 向目标地址请求得到所有区块
func queryAllBlocks(conn net.Conn) {
	defer conn.Close()
	conn.Write([]byte("queryAll," + MyPeer.peerAddr))

	data := make([]byte, 4096)
	n, err := conn.Read(data)
	if err != nil {
		fmt.Println(err)
	}

	//fmt.Println(n, string(data[:n]))

	var blocks []*blockchain.Block
	err = json.Unmarshal(data[:n], &blocks)
	if err != nil {
		fmt.Println(err)
	}

	// 如果获取到的区块链长度大于本链长度，则替换本链
	if len(blocks) > len(MyBlockchain.Blocks) ||
		blocks[0].TimeStamp < MyBlockchain.Blocks[0].TimeStamp {
		MyBlockchain.ReplaceChain(blocks)
	}

}

// 向目标地址发送添加区块
func addBlock(conn net.Conn, block *blockchain.Block) {
	defer conn.Close()
	blockByte, err := json.Marshal(block)
	if err != nil {
		fmt.Println(err)
	}

	tmp := [][]byte{
		[]byte("addBlock"),
		blockByte,
	}

	conn.Write(bytes.Join(tmp, []byte("")))
}

// 广播添加一个区块
func broadAddBlock(block *blockchain.Block) {
	for _, addr := range MyPeer.alivePeersAddr {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Println("send block to", addr, "failed")
			continue
		}
		addBlock(conn, block)
	}
}

// 向目标地址发送所有区块
func sendAllBlocks(conn net.Conn) {

	blocks := MyBlockchain.Blocks

	output, err := json.Marshal(blocks)
	if err != nil {
		fmt.Println(err)
	}
	conn.Write(output)
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	data := make([]byte, 4096)
	n, err := conn.Read(data)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(n, string(data[:n]))

	command := string(data[:n])
	switch {
	case strings.HasPrefix(command, "queryAll"):
		sendAllBlocks(conn)
		addr := strings.Split(command, ",")[1]
		//fmt.Println(addr)
		if netAddrCheck(addr) {
			MyPeer.alivePeersAddr = append(MyPeer.alivePeersAddr, addr)
		}

	case strings.HasPrefix(command, "addBlock"):
		var block blockchain.Block
		err = json.Unmarshal(data[8:n], &block)
		if err != nil {
			fmt.Println(err)
		} else {
			MyBlockchain.AppendBlock(&block)
		}
	}

}
