package blockchain

import "sync"

// Blockchain : 区块链结构
type Blockchain struct {
	Blocks []*Block
}

var mutex = &sync.Mutex{}

// NewBlockchain 创建一个新的区块
func NewBlockchain() *Blockchain {
	return &Blockchain{
		[]*Block{newGenesisBlock()}}
}

// AddBlock 给链上增加一个区块
func (blockchain *Blockchain) AddBlock(data string) {
	mutex.Lock()
	oldBlock := blockchain.Blocks[len(blockchain.Blocks)-1]
	newBlock := generateBlock(oldBlock, data)
	if isBlockValid(newBlock, oldBlock) {
		blockchain.Blocks = append(blockchain.Blocks, newBlock)
	}
	mutex.Unlock()
}

// AppendBlock 增加一个区块到链末
func (blockchain *Blockchain) AppendBlock(block *Block) {
	mutex.Lock()
	blockchain.Blocks = append(blockchain.Blocks, block)
	mutex.Unlock()
}

// ReplaceChain 替换当前区块链
func (blockchain *Blockchain) ReplaceChain(newBlocks []*Block) {
	mutex.Lock()
	blockchain.Blocks = newBlocks
	mutex.Unlock()
}
