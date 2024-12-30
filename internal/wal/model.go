package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
)

// WALState represents the current state of the Write-Ahead Log.
type WALState string

// WALConfig holds configuration options for the Write-Ahead Log.
type Config struct {
	MaxLogSegments          uint8              // Maximum number of log segment files allowed.
	MaxSegmentSize          uint32             // Maximum size of each log segment file in bytes.
	LogDirectory            string             // Directory where log segment files are stored.
	SegmentFilePrefix       string             // Prefix used for log segment file names.
	SyncInterval            time.Duration      // Interval for syncing data to disk in the background.
	OldSegmentDeletionLimit uint8              // Maximum number of old log segment files to delete when the total exceeds.
	EventListeners          *WALEventListeners // Event listeners for WAL operations.
}

type WAL struct {
	maxLogSegments uint8              // Maximum number of log segments allowed.
	maxSegmentSize uint32             // Maximum size of each log segment in bytes.
	logDirectory   string             // Directory where log segment files are stored.
	segmentPrefix  string             // Prefix used for log segment file names.
	listeners      *WALEventListeners // Event listeners for wal operations.

	totalSegment            uint8     // Total number of log segments.
	oldSegmentDeletionLimit uint8     // Limit on the number of old log segments to delete.
	activeSegmentId         uint64    // Id the currently active segment.
	lastLogSequenceNumber   uint64    // Last recorded log sequence number (LSN).
	segmentSize             uint32    // Size of the currently active segment in bytes.
	state                   WALState  // Current state of the wal.
	segment                 *os.File  // File handle for the active log segment.
	lastFsyncedAt           time.Time // Timestamp of the last successful data sync.

	syncTimer    *time.Ticker  // Ticker for periodic syncing.
	syncInterval time.Duration // Interval between background sync operations.
	buffer       *bufio.Writer // Buffered writer for efficient I/O operations.

	mutex      sync.Mutex         // Mutex to ensure thread-safe operations.
	context    context.Context    // Context for managing the wal lifecycle.
	cancelFunc context.CancelFunc // Function to cancel the wal context.
}

// WALEventListeners defines callback functions for various WAL events.
type WALEventListeners struct {
	// Triggered when a sync operation encounters an error.
	OnSyncError func(err error, segment *os.File)
	// Triggered at the start of a sync operation.
	OnSyncStart func(segmentID, lsn uint64, segment *os.File)
	// Triggered at the end of a sync operation.
	OnSyncEnd func(segmentID, lsn uint64, segment *os.File)
}

// WALCommand represents a command to be written to the WAL.
type WALCommand struct {
	Data              []byte `json:"data"`     // Command data payload.
	LogSequenceNumber uint64 `json:"lsn"`      // Log Sequence Number for this command.
	Checksum          uint32 `json:"checksum"` // CRC checksum for integrity verification of LSN and Data.
}
