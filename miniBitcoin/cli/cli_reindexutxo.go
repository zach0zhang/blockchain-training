package cli

import (
	"fmt"
	"miniBitcoin/blockchain"
)

func (cli *CLI) ReindexUTXO(nodeID string) {
	bc := blockchain.NewBlockchain(nodeID)
	UTXOSet := blockchain.UTXOSet{bc}
	UTXOSet.Reindex()

	count := UTXOSet.CountTransactions()
	fmt.Printf("DOne! There are %d transactions in the UTXO set.\n", count)
}
