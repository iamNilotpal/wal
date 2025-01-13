package main

import "github.com/iamNilotpal/wal/pkg/logger"

func main() {
	logger := logger.New("wal-service")
	logger.Info("starting wal service")
}
