package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

type WALOpts struct {
	// Total number of log files (max 255)
	MaxSegments uint8
	// Max size of a log file in bytes
	MaxSegmentSize uint64
	// Directory where log files will be stored
	LogDir string
	// Sync interval to disk
	SyncInterval time.Duration
	Logger       *zap.SugaredLogger
}

type WAL struct {
	// Total number of log files (max 255)
	maxSegments uint8
	// Max size of a log file in bytes
	mazSegmentSize uint64
	// Directory where log files will be stored
	logDir string
	log    *zap.SugaredLogger

	// Current LSN (Log Structured Number)
	currSegmentOffset uint8
	// Total log files
	segmentCount uint8
	// Current log file size
	segmentSize uint64
	// Current log file
	currSegment *os.File
	// File Sync interval
	syncTimer *time.Ticker
	// Used to efficiently write to file
	writeBuffer *bufio.Writer

	mutex      sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}
