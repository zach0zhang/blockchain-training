package main

import (
	"os"
	"os/signal"
	"syscall"
)

// 监听信号 ctrl+c 和 kill
func interruptListener() <-chan struct{} {
	c := make(chan struct{})
	go func() {
		interruptChannel := make(chan os.Signal, 1)
		signal.Notify(interruptChannel, os.Interrupt, syscall.SIGTERM)

		select {
		case sig := <-interruptChannel:
			log.Info("Received signal", sig, "Shutting down...")
		}
		close(c)

		// 继续监听信号，并输出正在关闭，证明程序在正常退出中
		for {
			select {
			case sig := <-interruptChannel:
				log.Info("Received signal", sig, "Already "+"shutting down...")
			}
		}
	}()

	return c
}
