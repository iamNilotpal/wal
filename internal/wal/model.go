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
	MaxLogSegments uint8
	MaxSegmentSize uint64
	LogDirName     string
	SegmentPrefix  string
	SyncInterval   time.Duration
	Logger         *zap.SugaredLogger
}

type WAL struct {
	maxLogSegments uint8
	maxSegmentSize uint64
	logDirName     string
	segmentPrefix  string
	log            *zap.SugaredLogger

	currSegmentId uint8
	totalSegment  uint8
	segmentSize   uint64
	currSegment   *os.File
	syncTimer     *time.Ticker
	writeBuffer   *bufio.Writer

	mutex      sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}
