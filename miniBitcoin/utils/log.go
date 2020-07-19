package utils

import (
	"errors"
	"fmt"
	"log"
	"os"
)

// LevelDebug ~ LevelPanic : 0 ~ 4
const (
	LevelDebug = iota
	LevelInfo
	LevelWarning
	LevelError
	LevelPanic
)

var logLevelMap = map[int]string{
	0: "Debug",
	1: "Info",
	2: "Warning",
	3: "Error",
	4: "Panic",
}

// Logger : log输出对象的结构体
type Logger struct {
	level    int
	fileName string
	l        *log.Logger
}

func (logger *Logger) SetLoggerLevel(level int) error {
	if level < LevelDebug || level > LevelError {
		str := fmt.Sprintf("input error level must set:\n")
		for i, v := range logLevelMap {
			str += fmt.Sprintf("%d: %s\t", i, v)
		}
		return errors.New(str)
	}
	logger.level = level
	return nil
}

func (logger *Logger) InitLogger(fileName string, level int) error {
	err := logger.SetLoggerLevel(level)
	if err != nil {
		return err
	}
	// 若未向方法中传入log文件路径，结构体中也没有log文件内容存储，则打印到标准输出
	if fileName == "" && logger.fileName == "" {
		logger.l = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		var file *os.File
		if fileName == "" {
			file, err = os.OpenFile(logger.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
		} else {
			file, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
		}
		logger.l = log.New(file, "", log.Ldate|log.Ltime|log.Lshortfile)
	}

	return nil
}

func (logger *Logger) Debug(args ...interface{}) {
	if logger.level <= LevelDebug {
		logger.l.SetPrefix("Debug ")
		logger.l.Output(2, fmt.Sprintln(args...))
	}
}

func (logger *Logger) Info(args ...interface{}) {
	if logger.level <= LevelInfo {
		logger.l.SetPrefix("Info ")
		logger.l.Output(2, fmt.Sprintln(args...))
	}
}

func (logger *Logger) Warning(args ...interface{}) {
	if logger.level <= LevelWarning {
		logger.l.SetPrefix("Warning ")
		logger.l.Output(2, fmt.Sprintln(args...))
	}
}

func (logger *Logger) Error(args ...interface{}) {
	if logger.level <= LevelError {
		logger.l.SetPrefix("Error ")
		logger.l.Output(2, fmt.Sprintln(args...))
	}
}

func (logger *Logger) Panic(args ...interface{}) {
	if logger.level <= LevelPanic {
		logger.l.SetPrefix("Panic ")
		logger.l.Output(2, fmt.Sprintln(args...))
	}
}
