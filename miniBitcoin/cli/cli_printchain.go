package cli

import (
	"fmt"
	"miniBitcoin/blockchain"
	"strconv"
)

func (cli *CLI) printChain(nodeID string) {
	bc := blockchain.NewBlockchain(nodeID)
	defer bc.Db.Close()

	bci := bc.Iterator()

	for {
		block := bci.Next()

		fmt.Printf("===========Block %x===========\n", block.Hash)
		fmt.Printf("Height: %d\n", block.Height)
		fmt.Printf("Prev.block %x\n", block.PrevBlockHash)
		pow := blockchain.NewProofOfWork(block)
		fmt.Printf("POW: %s\n\n", strconv.FormatBool(pow.Vaildate()))
		for _, tx := range block.Transactions {
			fmt.Println(tx)
		}
		fmt.Printf("\n\n")

		if len(block.PrevBlockHash) == 0 {
			break
		}
	}
}
