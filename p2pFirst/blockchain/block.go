package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// Block ： 区块的结构
type Block struct {
	Index     int32
	Version   int32
	Data      string
	TimeStamp int32
	Hash      [32]byte
	PrevHash  [32]byte
}

// BlockchainVersion 当前版本
const BlockchainVersion = 2

func newGenesisBlock() *Block {
	block := &Block{
		Index:     0,
		Version:   BlockchainVersion,
		Data:      "Genesis block",
		TimeStamp: int32(time.Now().Unix()),
	}
	block.Hash = calculateHash(block)
	return block
}

// 生成区块
func generateBlock(oldBlock *Block, data string) *Block {
	var newBlock Block

	newBlock.Index = oldBlock.Index + 1
	newBlock.Version = BlockchainVersion
	newBlock.Data = data
	newBlock.TimeStamp = int32(time.Now().Unix())
	newBlock.PrevHash = oldBlock.Hash

	newBlock.Hash = calculateHash(&newBlock)

	return &newBlock
}

// 计算当前区块哈希值
func calculateHash(block *Block) [32]byte {
	tmp := [][]byte{
		intToByte(block.Index),
		intToByte(block.Version),
		[]byte(block.Data),
		intToByte(block.TimeStamp),
		block.PrevHash[:],
	}
	hashbyte := bytes.Join(tmp, []byte(""))
	hash := sha256.Sum256(hashbyte)

	return hash
}

// 检查区块是否有效
func isBlockValid(newBlock, oldBlock *Block) bool {
	if oldBlock.Hash != newBlock.PrevHash ||
		calculateHash(newBlock) != newBlock.Hash ||
		newBlock.Index != oldBlock.Index+1 {
		return false
	}

	return true
}

func intToByte(num int32) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, num)
	if err != nil {
		fmt.Println("err: ", err)
		os.Exit(1)
	}
	return buffer.Bytes()
}
