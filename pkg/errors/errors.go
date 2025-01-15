package errors

import (
	"fmt"
	"time"
)

// ErrorCategory classifies different types of errors that can occur
// during WAL (Write-Ahead Log) operations. This helps in proper error
// handling, monitoring, and debugging of the system.
type ErrorCategory int

const (
	// ErrorStorage indicates errors related to underlying storage operations
	// such as file I/O, disk space, permissions, or filesystem issues.
	ErrorStorage ErrorCategory = iota + 1

	// ErrorCompression indicates errors during segment compression or
	// decompression operations, such as corrupt compressed data or
	// insufficient memory for compression buffers.
	ErrorCompression

	// ErrorCheckpoint indicates errors during checkpoint operations,
	// such as failed snapshot creation or inconsistent state.
	ErrorCheckpoint

	// ErrorRecovery indicates errors during WAL recovery process,
	// such as corrupt segments or missing files needed for recovery.
	ErrorRecovery

	// ErrorBatch indicates errors related to batch operations,
	// such as batch size limits or invalid batch data.
	ErrorBatch
)

// String returns the string representation of the error category.
// This is useful for logging, metrics, and error reporting.
func (c ErrorCategory) String() string {
	switch c {
	case ErrorStorage:
		return "storage"
	case ErrorCompression:
		return "compression"
	case ErrorCheckpoint:
		return "checkpoint"
	case ErrorRecovery:
		return "recovery"
	case ErrorBatch:
		return "batch"
	default:
		return "unknown"
	}
}

type WALError struct {
	Err       error
	Operation string
	Timestamp time.Time
	Category  ErrorCategory
}

func (e *WALError) Error() string {
	return fmt.Sprintf("[%v] %s: %v : %s", e.Category, e.Operation, e.Err, e.Timestamp.String())
}

// IsRetryAble returns whether errors of this category can be retried.
// This helps callers decide whether to retry failed operations.
func (e *WALError) IsRetryAble() bool {
	switch e.Category {
	case ErrorStorage:
		// Storage errors might be temporary (e.g., disk full, network issues).
		return true
	case ErrorCompression:
		// Compression errors are usually not retry able (corrupted data).
		return false
	case ErrorCheckpoint:
		// Checkpoint errors might be temporary (e.g., resource constraints).
		return true
	case ErrorRecovery:
		// Recovery errors are usually not retry able (corrupted WAL).
		return false
	case ErrorBatch:
		// Batch errors might be temporary (e.g., size limits).
		return true
	default:
		return false
	}
}
