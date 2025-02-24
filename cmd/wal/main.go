package main

import (
	"context"
	"os"

	"github.com/iamNilotpal/wal/internal/core/services/wal"
	"github.com/iamNilotpal/wal/pkg/errors"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal-service")
	defer logger.Sync()

	logger.Info("starting wal service")

	wal, err := wal.New(context.Background(), nil)
	if err != nil {
		if errors.IsValidationError(err) {
			err := errors.GetValidationError(err)
			logger.Infow("create wal error", "field", err.Field, "value", err.Value, "error", err.Err)
		} else {
			logger.Infow("create wal error", "error", err)
		}
		os.Exit(1)
	}

	if err := wal.Write(
		context.Background(),
		[]byte(`
			EDA makes real-time processing possible by acting on events as soon as they occur. Traditional request-response systems, by comparison, must wait for a client request to initiate any action. In the context of an e-commerce system, a request-response model could lead to overselling if multiple purchase requests for the same item are made simultaneously, and the inventory isn't updated until the responses are processed. EDA, with its real-time event handling, eliminates this risk by immediately updating the inventory upon receiving a purchase event. Developers could set up listeners for specific events (like a purchase event), ensuring data accuracy and timeliness that would be difficult to achieve with a request-response system.
		`),
		false,
	); err != nil {
		logger.Infow("write error", "error", err)
	}

	data, err := wal.ReadAt(context.Background(), 436)
	if err != nil {
		logger.Error("read error", err)
	} else {
		logger.Infow("entry data", "data", data)
	}

	if err := wal.Close(context.Background()); err != nil {
		logger.Infow("error closing wal", "error", err)
	}
}
