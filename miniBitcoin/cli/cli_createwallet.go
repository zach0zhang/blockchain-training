package cli

import (
	"fmt"
	"miniBitcoin/blockchain"
)

func (cli *CLI) CreateWallet(nodeID string) {
	wallets, _ := blockchain.NewWallets(nodeID)
	address := wallets.CreateWallet()
	wallets.SaveToFile(nodeID)

	fmt.Printf("Your new address: %s\n", address)
}
