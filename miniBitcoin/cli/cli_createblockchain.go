package cli

import (
	"log"
	"miniBitcoin/blockchain"
)

func (cli *CLI) createBlockchain(address, nodeID string) {
	if !blockchain.ValidateAddress(address) {
		log.Panic("ERROR: Address is not valid")
	}

	bd := blockchain.CreateBlockchain(address, nodeID)
	defer bd.Db.Close()
}
