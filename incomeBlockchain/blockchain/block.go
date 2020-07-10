package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// BlockchainVersion 当前版本
const BlockchainVersion = 1

// Block 区块的结构
type Block struct {
	Version       int32    // 版本号
	PrevBlockHash [32]byte // 前一区块哈希值
	Income        int32    // 收入与支出
	Hash          [32]byte // 哈希值
	TimeStamp     int32    // 时间戳
}

// 创建创始区块
func newGenesisBlock() *Block {
	block := &Block{
		Version:   BlockchainVersion,
		Income:    0,
		TimeStamp: int32(time.Now().Unix()),
	}
	block.Hash = calculateHash(block)
	return block
}

// 生成区块
func generateBlock(oldBlock *Block, income int32) *Block {
	var newBlock Block

	newBlock.Version = BlockchainVersion
	newBlock.PrevBlockHash = oldBlock.Hash
	newBlock.Income = income
	newBlock.TimeStamp = int32(time.Now().Unix())
	newBlock.Hash = calculateHash(&newBlock)

	return &newBlock
}

// 计算当前区块哈希值
func calculateHash(block *Block) [32]byte {
	tmp := [][]byte{
		intToByte(block.Version),
		block.PrevBlockHash[:],
		intToByte(block.Income),
		intToByte(block.TimeStamp)}
	hashbyte := bytes.Join(tmp, []byte(""))
	hash := sha256.Sum256(hashbyte)

	return hash
}

// 检查区块是否有效
func isBlockValid(newBlock, oldBlock *Block) bool {
	if oldBlock.Hash != newBlock.PrevBlockHash ||
		calculateHash(newBlock) != newBlock.Hash {
		return false
	}

	return true
}

// int32 转化为 []byte
func intToByte(num int32) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, num)
	if err != nil {
		fmt.Println("err: ", err)
		os.Exit(1)
	}
	return buffer.Bytes()
}
