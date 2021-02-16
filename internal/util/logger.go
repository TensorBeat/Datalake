package util

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func MakeLogger() *zap.SugaredLogger {
	//Setup Logging
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	loggerMgr, err := config.Build()

	if err != nil {
		log.Fatalf("Couldn't start zap logger: %v", err)
	}

	logger := loggerMgr.Sugar()

	return logger
}
