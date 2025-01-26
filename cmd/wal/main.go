package main

import (
	"context"
	"os"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/wal"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal-service")
	defer logger.Sync()

	logger.Info("starting wal service")

	wal, err := wal.New(&domain.WALOptions{
		RetentionDays:   10,
		MinSegmentsKept: 10,
		MaxSegmentsKept: 20,
		SyncOnFlush:     true,
		SyncOnWrite:     true,
		BufferSize:      8191,
	})

	if err != nil {
		logger.Infow("create wal error", "error", err)
		logger.Sync()
		os.Exit(1)
	}

	if err := wal.Close(context.Background()); err != nil {
		logger.Infow("error closing wal", "error", err)
	}
}
