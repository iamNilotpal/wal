package errors

import (
	"fmt"
	"runtime"
	"strings"
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

	// ErrorSegment indicates errors related to segment operations,
	// such as segment creation, rotation, or deletion.
	ErrorSegment
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
	case ErrorSegment:
		return "segment"
	default:
		return "unknown"
	}
}

// WALError represents an error with rich context for WAL operations.
// It provides additional metadata such as error category, operation details,
// stack trace, and additional contextual fields to aid in debugging.
type WALError struct {
	Cause     error          `json:"cause"`     // Underlying error.
	Message   string         `json:"message"`   // Original error message.
	Category  ErrorCategory  `json:"category"`  // Classification of error type.
	Timestamp time.Time      `json:"timestamp"` // Timestamp when the error occurred.
	Fields    map[string]any `json:"fields"`    // Key-value pairs for additional context.
	Stack     string         `json:"stack"`     // Stack trace at the point of error creation.
	SegmentID uint64         `json:"segmentId"` // Segment ID, if related to segment operations.
	FilePath  string         `json:"filePath"`  // File path, useful for storage-related errors.
	Operation string         `json:"operation"` // Operation being performed when the error occurred.
}

// WALErrorBuilder provides a structured way to construct WALError instances.
type WALErrorBuilder struct {
	err WALError
}

// NewWALError initializes a new WALErrorBuilder with a given message.
func NewWALError(msg string) *WALErrorBuilder {
	builder := &WALErrorBuilder{
		err: WALError{
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    make(map[string]interface{}),
		},
	}

	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	builder.err.Stack = string(buf[:n])

	return builder
}

// WithCause sets the underlying cause of the error.
func (b *WALErrorBuilder) WithCause(err error) *WALErrorBuilder {
	b.err.Cause = err
	return b
}

// WithCategory sets the category of the error.
func (b *WALErrorBuilder) WithCategory(category ErrorCategory) *WALErrorBuilder {
	b.err.Category = category
	return b
}

// WithOperation sets the operation being performed when the error occurred.
func (b *WALErrorBuilder) WithOperation(op string) *WALErrorBuilder {
	b.err.Operation = op
	return b
}

// WithField adds additional context as key-value pairs.
func (b *WALErrorBuilder) WithField(key string, value any) *WALErrorBuilder {
	b.err.Fields[key] = value
	return b
}

// WithSegmentID sets the segment ID for segment-related errors.
func (b *WALErrorBuilder) WithSegmentID(id uint64) *WALErrorBuilder {
	b.err.SegmentID = id
	return b
}

// WithFilePath sets the file path for storage-related errors.
func (b *WALErrorBuilder) WithFilePath(path string) *WALErrorBuilder {
	b.err.FilePath = path
	return b
}

// Build constructs and returns the final WALError instance.
func (b *WALErrorBuilder) Build() *WALError {
	return &b.err
}

// Error implements the error interface for WALError.
// It returns a detailed error message with relevant metadata.
func (e *WALError) Error() string {
	var parts []string

	parts = append(parts, fmt.Sprintf("Error: %s", e.Message))

	if e.Category != 0 {
		parts = append(parts, fmt.Sprintf("Category: %s", e.Category))
	}

	if e.Operation != "" {
		parts = append(parts, fmt.Sprintf("Operation: %s", e.Operation))
	}

	if e.SegmentID != 0 {
		parts = append(parts, fmt.Sprintf("SegmentID: %d", e.SegmentID))
	}

	if e.FilePath != "" {
		parts = append(parts, fmt.Sprintf("FilePath: %s", e.FilePath))
	}

	parts = append(parts, fmt.Sprintf("Time: %s", e.Timestamp.Format(time.RFC3339)))

	if len(e.Fields) > 0 {
		fieldParts := []string{}
		for k, v := range e.Fields {
			fieldParts = append(fieldParts, fmt.Sprintf("%s=%v", k, v))
		}
		parts = append(parts, fmt.Sprintf("Fields: {%s}", strings.Join(fieldParts, ", ")))
	}

	if e.Cause != nil {
		parts = append(parts, fmt.Sprintf("Cause: %v", e.Cause))
	}

	return strings.Join(parts, " | ")
}

// IsRetryable returns whether errors of this category can be retried.
// This helps callers decide whether to retry failed operations.
func (e *WALError) IsRetryable() bool {
	switch e.Category {
	case ErrorStorage:
		// Storage errors might be temporary (e.g., disk full, network issues).
		return true
	case ErrorCompression:
		// Compression errors are usually not retryable (corrupted data).
		return false
	case ErrorCheckpoint:
		// Checkpoint errors might be temporary (e.g., resource constraints).
		return true
	case ErrorRecovery:
		// Recovery errors are usually not retryable (corrupted WAL).
		return false
	case ErrorBatch:
		// Batch errors might be temporary (e.g., size limits).
		return true
	case ErrorSegment:
		// Segment errors might be temporary (e.g., resource constraints).
		return true
	default:
		return false
	}
}

// IsWALError checks if an error is a WALError
// This utility function makes it easier to check the error type
func IsWALError(err error) bool {
	_, ok := err.(*WALError)
	return ok
}

// AsWALError attempts to convert an error to a WALError
// If the error is not a WALError, it returns nil, false
// This utility function makes it easier to extract WALError details
func AsWALError(err error) (*WALError, bool) {
	walErr, ok := err.(*WALError)
	return walErr, ok
}
