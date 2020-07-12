package blockchain

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestBlockchain(t *testing.T) {
	blockchain1 := NewBlockchain()
	blockchain2 := NewBlockchain()

	println("blockchian1:")
	printBlockchain(blockchain1.Blocks)
	println("blockchain2:")
	printBlockchain(blockchain2.Blocks)

	blockchain1.AddBlock("blockchain1 add block1")
	blockchain1.AddBlock("blockchain1 add block2")
	blockchain2.AddBlock("blockchain2 add block2")

	println("blockchian1:")
	printBlockchain(blockchain1.Blocks)
	println("blockchain2:")
	printBlockchain(blockchain2.Blocks)

	blockchain2.ReplaceChain(blockchain1.Blocks)
	println("blockchian1:")
	printBlockchain(blockchain1.Blocks)
	println("blockchain2:")
	printBlockchain(blockchain2.Blocks)
}

func printBlockchain(blocks []*Block) {

	for i, v := range blocks {
		fmt.Printf(`Block%d:
	Index: %d
	Version: %d
	PrevHash: %s
	Data: %s
	Hash: %s
	TimeStamp: %d
`,
			i, v.Index, v.Version, hex.EncodeToString(v.PrevHash[:]), v.Data, hex.EncodeToString(v.Hash[:]), v.TimeStamp)
	}
}
