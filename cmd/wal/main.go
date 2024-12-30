package main

import "github.com/iamNilotpal/wal/pkg/logger"

func main() {
	logger := logger.New("wal")
	logger.Info("Starting wal service")
}
