package main

import (
	"context"
	"os"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/wal"
	"github.com/iamNilotpal/wal/pkg/errors"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal-service")
	defer logger.Sync()

	logger.Info("starting wal service")

	wal, err := wal.New(context.Background(), &domain.WALOptions{})
	if err != nil {
		if errors.IsValidationError(err) {
			err := errors.GetValidationError(err)
			logger.Infow("create wal error", "field", err.Field, "value", err.Value, "error", err.Err)
		} else {
			logger.Infow("create wal error", "error", err)
		}
		os.Exit(1)
	}

	data, err := wal.ReadAll(context.Background())
	if err != nil {
		logger.Errorw("read error", "error", err)
	}

	logger.Infow("data retrieved", "data", data)

	if err := wal.Close(context.Background()); err != nil {
		logger.Infow("error closing wal", "error", err)
	}
}
