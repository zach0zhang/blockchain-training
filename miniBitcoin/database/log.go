package database

import (
	"miniBitcoin/utils"
)

var log utils.Logger

func init() {
	log.InitLogger("database.log", utils.LevelDebug)
}
