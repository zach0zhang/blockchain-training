package cli

import (
	"fmt"
	"log"
	"miniBitcoin/blockchain"
)

func (cli *CLI) ListAddresses(nodeID string) {
	wallets, err := blockchain.NewWallets(nodeID)
	if err != nil {
		log.Panic(err)
	}

	addresses := wallets.GetAddresses()

	for _, address := range addresses {
		fmt.Println(address)
	}
}
