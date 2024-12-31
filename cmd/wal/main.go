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
		&wal.Config{
			MaxLogSegments: 20,
			LogDirectory:   "logs",
			SyncInterval:   time.Second * 5,
			MaxSegmentSize: 10485760, // 10MB
		},
	)
	if err != nil {
		logger.Fatalln("error", err)
	}
	defer wal.Close()

	state := wal.State()
	logger.Infow("state", "state", state)

	wal.Write([]byte("Hello World - 1"))
	wal.Write([]byte("Hello World - 2"))
	wal.Write([]byte("Hello World - 3"))
}
