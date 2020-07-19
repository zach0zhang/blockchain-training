package utils

import "testing"

func TestLog(T *testing.T) {
	var log1 Logger
	var log2 Logger
	log1.InitLogger("log1.txt", LevelDebug)
	log2.InitLogger("log2.txt", LevelWarning)
	for i := 0; i < 100; i++ {
		log1.Debug("hello log1", i)
		log2.Debug("hello log2", i)
		log1.Info("hello log1", i)
		log2.Info("hello log2", i)
		log1.Warning("hello log1", i)
		log2.Warning("hello log2", i)
		log1.Error("hello log1", i)
		log2.Error("hello log2", i)
		log1.Panic("hello log1", i)
		log2.Panic("hello log2", i)
	}
	var log3 Logger
	log3.InitLogger("", LevelDebug)
	for i := 0; i < 10; i++ {
		log3.Debug("hello log3", i)
		log3.Info("hello log3", i)
		log3.Warning("hello log3", i)
		log3.Error("hello log3", i)
		log3.Panic("hello log3", i)
	}

}
