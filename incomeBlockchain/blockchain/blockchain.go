package blockchain

// Blockchain 区块链的结构
type Blockchain struct {
	Blocks []*Block
}

// func (blockchain *Blockchain)replaceChain(newBlockChain []*Block) {
// 	if len(newBlockChain) > len(blockchain) {
// 		blockchain = newBlockChain
// 	}
// }

// NewBlockchain 创建一个新的区块链
func NewBlockchain() *Blockchain {
	return &Blockchain{
		[]*Block{newGenesisBlock()}}
}

// AddBlock 给链上增加一个区块
func (blockchain *Blockchain) AddBlock(income int32) {
	oldBlock := blockchain.Blocks[len(blockchain.Blocks)-1]
	newBlock := generateBlock(oldBlock, income)
	if isBlockValid(newBlock, oldBlock) {
		blockchain.Blocks = append(blockchain.Blocks, newBlock)
	}
}
