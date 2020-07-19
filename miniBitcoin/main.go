package main

import "os"

func main() {
	// cli := cli.CLI{}
	// cli.Run()

	err := miniBitcoinMain()
	if err != nil {
		os.Exit(1)
	}
}
