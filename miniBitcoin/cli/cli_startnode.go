package cli

import (
	"fmt"
	"log"
	"miniBitcoin/blockchain"
)

func (cli *CLI) startNode(nodeID, minerAddress string) {
	fmt.Printf("Starting node %s\n", nodeID)
	if len(minerAddress) > 0 {
		if blockchain.ValidateAddress(minerAddress) {
			fmt.Println("Mining is on. Address to receive reward:", minerAddress)
		} else {
			log.Panic("Wrong miner address")
		}
	}
	blockchain.StartServer(nodeID, minerAddress)
}
