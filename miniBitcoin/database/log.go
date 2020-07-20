package database

import (
	"miniBitcoin/utils"
)

var log utils.Logger

func init() {
	log.InitLogger("database.log", utils.LevelDebug)
	for _, drv := range drivers {
		if drv.UseLogger != nil {
			drv.UseLogger(log)
		}
	}
}
