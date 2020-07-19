package main

func miniBitcoinMain() error {

	// 监听关闭程序信号
	interrupt := interruptListener()
	defer log.Info("Shutdown complete")

	<-interrupt
	return nil
}
