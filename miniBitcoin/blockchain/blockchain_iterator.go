package blockchain

import (
	"log"

	"github.com/boltdb/bolt"
)

type BlockchainIterator struct {
	CurrentHash []byte
	Db          *bolt.DB
}

func (i *BlockchainIterator) Next() *Block {
	var block *Block

	err := i.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(blocksBucket))
		encodedBlock := b.Get(i.CurrentHash)
		block = DeserializeBlock(encodedBlock)

		return nil
	})

	if err != nil {
		log.Panic(err)
	}

	i.CurrentHash = block.PrevBlockHash

	return block
}
