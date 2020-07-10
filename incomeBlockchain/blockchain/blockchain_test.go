package blockchain

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestBlockChain(t *testing.T) {
	blockchain := NewBlockchain()

	blockchain.AddBlock(123)
	blockchain.AddBlock(456)

	for i, v := range blockchain.Blocks {
		fmt.Printf(`Block%d:
	Version: %d
	PrevBlockHash: %s
	Income: %d
	Hash: %s
	TimeStamp: %d
`,
			i, v.Version, hex.EncodeToString(v.PrevBlockHash[:]), v.Income, hex.EncodeToString(v.Hash[:]), v.TimeStamp)
	}
}
