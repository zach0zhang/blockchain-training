package network

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type Peer struct {
	apiHTTPAddr    string
	peerAddr       string
	otherPeersAddr string
}

var MyPeer Peer

func init() {
	flag.StringVar(&MyPeer.apiHTTPAddr, "api", "", "api http server address")
	flag.StringVar(&MyPeer.peerAddr, "addr", "", "peer server address")
	flag.StringVar(&MyPeer.otherPeersAddr, "peers", "", "other peers address")
	flag.Parse()

	MyPeer.connectToPeers()
	MyPeer.startServer()
}

// 连接其他节点
func (peer *Peer) connectToPeers() {
	peersAddr := strings.Split(peer.otherPeersAddr, ",")

	for _, p := range peersAddr {

		if netAddrCheck(p) { // 地址合法则开始连接

			conn, err := net.DialTimeout("tcp", p, 3*time.Second)
			if err != nil {
				fmt.Println("connect to", p, "failed", err)
				continue
			}
			defer conn.Close()

			queryAllBlocks(conn)
		}
	}
}

// 建立apiHTTP服务与节点服务
func (peer *Peer) startServer() {
	http.HandleFunc("/", handlerUI)
	go func() {
		fmt.Println("listen HTTP on", MyPeer.apiHTTPAddr)
		err := http.ListenAndServe(MyPeer.apiHTTPAddr, nil)
		checkErr(err)
	}()

	fmt.Println("listen peer on", MyPeer.peerAddr)
	server, err := net.Listen("tcp", MyPeer.peerAddr)
	checkErr(err)
	defer server.Close()

	for {
		conn, err := server.Accept()
		checkErr(err)

		go handleConn(conn)
	}
}

func handlerUI(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello world"))
}
