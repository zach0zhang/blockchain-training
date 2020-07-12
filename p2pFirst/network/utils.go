package network

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func netAddrCheck(addr string) bool {
	items := strings.Split(addr, ":")
	if items == nil || len(items) != 2 {
		return false
	}

	a := net.ParseIP(items[0])
	if a == nil {
		return false
	}

	match, err := regexp.MatchString("^[0-9]*$", items[1])
	if err != nil {
		return false
	}

	i, err := strconv.ParseInt(items[1], 10, 64)
	if err != nil {
		return false
	}
	if i < 0 || i > 65535 {
		return false
	}

	if match == false {
		return false
	}

	return true
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
