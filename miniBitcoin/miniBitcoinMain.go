package main

import (
	"errors"
	"miniBitcoin/database"
	_ "miniBitcoin/database/btcdb"
	"os"
)

var dbPath = "miniBtcDB"
var dbType = "btcdb"

func loadBlockDB() (database.DB, error) {
	log.Info("Loading block database from", dbPath)

	if !database.AlreadyRegister(dbType) {
		return nil, errors.New("database is not exist")
	}

	db, err := database.Open(dbType, dbPath)
	if err != nil {
		err = os.MkdirAll(dbPath, 0700)
		if err != nil {
			return nil, err
		}

		db, err = database.Create(dbType, dbPath)
		if err != nil {
			return nil, err
		}
	}
	log.Info("Block database loaded")
	return db, nil
}

func miniBitcoinMain() error {

	// 监听关闭程序信号
	interrupt := interruptListener()
	defer log.Info("Shutdown complete")

	// 加载数据库
	db, err := loadBlockDB()
	if err != nil {
		log.Error(err)
		return err
	}
	defer func() {
		log.Info("Gracefully shutting down the database...")
		db.Close()
	}()

	<-interrupt
	return nil
}
