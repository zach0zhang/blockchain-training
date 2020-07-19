package main

import (
	"miniBitcoin/utils"
)

var log utils.Logger

func init() {
	log.InitLogger("miniBitcoin.log", utils.LevelDebug)
}
