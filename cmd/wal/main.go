package main

import (
	"time"

	"github.com/iamNilotpal/wal/internal/wal"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal")
	logger.Info("starting wal service")

	wal, err := wal.New(
		&wal.WALOpts{
			MaxLogSegments: 20,
			LogDirName:     "logs",
			Logger:         logger,
			MaxSegmentSize: 10485760,
			SyncInterval:   time.Second * 5,
		},
	)
	if err != nil {
		logger.Fatalln("err", err)
	}

	defer wal.Close()
}
