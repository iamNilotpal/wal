package main

import (
	"context"
	"os"
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/wal"
	"github.com/iamNilotpal/wal/pkg/errors"
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
		BufferSize:      8192,
	})

	if err != nil {
		if errors.IsValidationError(err) {
			ve := errors.GetValidationError(err)
			logger.Infow("create wal error", "field", ve.Field, "value", ve.Value, "error", ve.Err)
		} else {
			logger.Infow("create wal error", "error", err)
		}

		logger.Sync()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := wal.Write(
		ctx,
		[]byte(`
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
				&domain.WALOptions{
					RetentionDays:   10,
					MinSegmentsKept: 10,
					MaxSegmentsKept: 20,
					SyncOnFlush:     true,
					SyncOnWrite:     true,
					BufferSize:      8192,
				}
	`),
		true,
	); err != nil {
		logger.Infow("create wal error", "error", err)
	}

	if err := wal.Close(context.Background()); err != nil {
		logger.Infow("error closing wal", "error", err)
	}
}
