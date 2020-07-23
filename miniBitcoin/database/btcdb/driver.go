package btcdb

import (
	"errors"
	"fmt"
	"miniBitcoin/database"
	"miniBitcoin/utils"
)

var log utils.Logger

const dbType = "btcdb"

func init() {
	driver := database.Driver{
		DbType:    dbType,
		Create:    createDBDriver,
		Open:      openDBDriver,
		UseLogger: useLogger,
	}
	fmt.Println("init btcdb")
	if err := database.RegisterDriver(driver); err != nil {
		panic(fmt.Sprintf("Failed to register database driver '%s': %v", dbType, err))
	}
}

func parseArgs(funcName string, args ...interface{}) (string, error) {
	if len(args) != 1 {
		return "", errors.New("invalid arguments to " + dbType + "." + funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", errors.New("first argument" + dbType + "." + "funcName" + "is invalid")
	}

	return dbPath, nil
}

func openDBDriver(args ...interface{}) (database.DB, error) {
	dbPath, err := parseArgs("Open", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, false)
}

func createDBDriver(args ...interface{}) (database.DB, error) {
	dbPath, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, true)
}

func useLogger(logger utils.Logger) {
	log = logger
}
