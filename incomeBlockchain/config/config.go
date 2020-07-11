package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Configuration struct represents the configuration
type Configuration struct {
	WebName      string
	Address      string
	ReadTimeout  int64
	WriteTimeout int64
}

// Cfg data
var Cfg Configuration

func checkError(err error) {
	if err != nil {
		fmt.Println("err:", err)
		os.Exit(1)
	}
}

// InitConfig 初始化配置
func InitConfig(filePath string) {
	file, err := os.Open(filePath)
	checkError(err)

	decoder := json.NewDecoder(file)
	Cfg = Configuration{}
	err = decoder.Decode(&Cfg)
	checkError(err)
}
