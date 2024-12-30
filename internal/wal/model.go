package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
)

type WALState string

type Config struct {
	// Max log segment files.
	MaxLogSegments uint8
	// Max log segment file size.
	MaxSegmentSize uint32
	// Directory to store log files.
	LogDirName string
	// Log segment prefix.
	SegmentPrefix string
	// Background data sync to disk interval.
	SyncInterval time.Duration
	// Event listeners.
	Listeners *WalEventListeners
}

type WAL struct {
	maxLogSegments uint8
	maxSegmentSize uint32
	logDirName     string
	segmentPrefix  string
	listeners      *WalEventListeners

	totalSegment      uint8
	currSegmentFileId uint64
	lastLSN           uint64
	segmentSize       uint32
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
	OnSyncStart func(currSegmentFileId uint64, lastSequenceId uint64, currSegmentFile *os.File)
	OnSyncEnd   func(currSegmentFileId uint64, lastSequenceId uint64, currSegmentFile *os.File)
}

type WALCommand struct {
	// Log Sequence Number.
	LSN uint64 `json:"lsn"`
	// Origin Data.
	Data []byte `json:"data"`
	// CRC Checksum of LSN and Data.
	Checksum uint32 `json:"checksum"`
}
