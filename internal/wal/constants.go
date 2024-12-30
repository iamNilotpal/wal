package wal

import "time"

const (
	startLSN          uint64        = 1
	startSegmentId    uint8         = 0
	segmentPrefix     string        = "segment-"
	autoFsyncDuration time.Duration = time.Second * 15
)

const (
	StateIdle         WALState = "idle"
	StateSyncing      WALState = "syncing"
	StateAppend       WALState = "append-buffer"
	StateInitializing WALState = "initializing"
)
