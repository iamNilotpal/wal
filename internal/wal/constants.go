package wal

import "time"

const (
	startLSN          uint64        = 1
	startSegmentId    uint64        = 0
	segmentPrefix     string        = "segment-"
	autoFsyncDuration time.Duration = time.Second * 15
)

const (
	StateIdle         WALState = "IDLE"
	StateClosing      WALState = "CLOSING"
	StateInitializing WALState = "INITIALIZING"

	StateWriteData WALState = "WRITE_DATA"

	StateManualSync     WALState = "MANUAL_SYNC"
	StateBackgroundSync WALState = "BACKGROUND_SYNC"
)
