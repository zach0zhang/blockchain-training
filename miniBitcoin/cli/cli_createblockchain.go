package cli

import (
	"fmt"
	"log"
	"miniBitcoin/blockchain"
)

func (cli *CLI) createBlockchain(address, nodeID string) {
	if !blockchain.ValidateAddress(address) {
		log.Panic("ERROR: Address is not valid")
	}

	bc := blockchain.CreateBlockchain(address, nodeID)
	defer bc.Db.Close()

	UTXOSet := blockchain.UTXOSet{bc}
	UTXOSet.Reindex()

	fmt.Println("Done")
}
