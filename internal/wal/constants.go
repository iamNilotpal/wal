package wal

import "time"

const (
	startLogSequenceNumber  uint64        = 0
	startSegmentId          uint64        = 0
	oldSegmentDeletionLimit uint8         = 5
	segmentPrefix           string        = "segment-"
	autoFsyncDuration       time.Duration = time.Second * 15
)

const (
	StateIdle         WALState = "IDLE"         // WAL is inactive and ready for operations.
	StateInitializing WALState = "INITIALIZING" // WAL is being initialized or started.
	StateClosing      WALState = "CLOSING"      // WAL is gracefully shutting down.
	StateClosed       WALState = "CLOSED"       // WAL has been closed and is no longer operational.

	StateWriting     WALState = "WRITING"      // Actively writing data to the WAL.
	StateWriteQueued WALState = "WRITE_QUEUED" // Write operation queued, awaiting processing.
	StateWriteError  WALState = "WRITE_ERROR"  // Error occurred during a write operation.

	StateSyncPending   WALState = "SYNC_PENDING"   // Sync operation pending, awaiting trigger.
	StateSyncing       WALState = "SYNCING"        // Sync operation in progress.
	StateSyncCompleted WALState = "SYNC_COMPLETED" // Sync operation successfully completed.
	StateSyncFailed    WALState = "SYNC_FAILED"    // Sync operation failed.

	StateRecovering    WALState = "RECOVERING"     // WAL is recovering from an unexpected failure.
	StateRecoveryError WALState = "RECOVERY_ERROR" // Error occurred during recovery.

	StateError WALState = "ERROR" // General error state for unforeseen issues.
)
