package config

import (
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	InitConfig("config.json")
	fmt.Println(Cfg)
}
