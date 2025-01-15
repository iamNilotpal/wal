package main

import (
	"os"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/wal"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal-service")
	defer logger.Sync()

	logger.Info("Starting wal service")

	wal, err := wal.New(&domain.WALOptions{})
	if err != nil {
		logger.Infow("create wal error", "error", err)
		logger.Sync()
		os.Exit(1)
	}

	if err := wal.Close(); err != nil {
		logger.Infow("error closing wal", "error", err)
	}
}
