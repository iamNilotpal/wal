package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
)

type WALState string

type WALOpts struct {
	MaxLogSegments uint8
	MaxSegmentSize uint64
	LogDirName     string
	SegmentPrefix  string
	SyncInterval   time.Duration
	Listeners      *WalEventListeners
}

type WAL struct {
	maxLogSegments uint8
	maxSegmentSize uint64
	logDirName     string
	segmentPrefix  string
	listeners      *WalEventListeners

	currSegmentFileId uint8
	totalSegment      uint8
	lastLSN           uint64
	segmentSize       uint64
	state             WALState
	currSegmentFile   *os.File
	lastFsyncedAt     time.Time
	syncTimer         *time.Ticker
	syncInterval      time.Duration
	writeBuffer       *bufio.Writer

	mutex      sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}

type WalEventListeners struct {
	OnSyncError func(error, *os.File)
	OnSyncStart func(currSegmentFileId uint8, lastSequenceId uint64, currSegmentFile *os.File)
	OnSyncEnd   func(currSegmentFileId uint8, lastSequenceId uint64, currSegmentFile *os.File)
}

type WalData struct {
	SequenceId uint64 `json:"sequenceId"`
	Data       []byte `json:"data"`
	Checksum   uint32 `json:"checksum"`
}
