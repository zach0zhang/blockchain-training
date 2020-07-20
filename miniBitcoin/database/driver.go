package database

import (
	"errors"
	"fmt"
	"miniBitcoin/utils"
)

type Driver struct {
	DbType string

	Create func(args ...interface{}) (DB, error)

	Open func(args ...interface{}) (DB, error)

	UseLogger func(logger utils.Logger)
}

var drivers = make(map[string]*Driver)

// RegisterDriver ： 将数据库注册进drivers中
func RegisterDriver(driver Driver) error {
	_, exists := drivers[driver.DbType]
	if exists {
		str := fmt.Sprintf("driver %q is already registered", driver.DbType)
		return errors.New(str)
	}

	drivers[driver.DbType] = &driver
	return nil
}

// GetRegisterDrivers ： 得到已经注册在drivers中的数据库
func GetRegisterDrivers() []string {
	registerDBs := make([]string, 0, len(drivers))
	for _, drv := range drivers {
		registerDBs = append(registerDBs, drv.DbType)
	}
	return registerDBs
}

// AlreadyRegister : 检查dbType是否已经注册
func AlreadyRegister(dbType string) bool {
	_, exists := drivers[dbType]
	if exists {
		return true
	}
	return false
}

func Create(dbType string, args ...interface{}) (DB, error) {
	drv, exists := drivers[dbType]
	if !exists {
		str := fmt.Sprintf("driver %q is not registered", dbType)
		return nil, errors.New(str)
	}

	return drv.Create(args...)
}

func Open(dbType string, args ...interface{}) (DB, error) {
	drv, exists := drivers[dbType]
	if !exists {
		str := fmt.Sprintf("driver %q is not registered", dbType)
		return nil, errors.New(str)
	}

	return drv.Open(args...)
}
