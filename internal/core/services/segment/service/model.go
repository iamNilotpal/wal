package segment

import (
	"bufio"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
	"github.com/iamNilotpal/wal/pkg/pool"
)

// Segment represents a single log file on disk that stores sequential data.
// It provides thread-safe operations for writing log entries and managing segment state.
type Segment struct {
	// Configuration options for the WAL system, including segment behavior,
	// compression settings, and checksum parameters.
	options *domain.WALOptions

	// Interfaces for data integrity and compression operations.
	fs         ports.FileSystemPort  // Handles File System operations.
	checksum   ports.ChecksumPort    // Handles data integrity verification.
	compressor ports.CompressionPort // Handles data compression/decompression.

	// Core segment properties
	id         uint64           // Unique monotonically increasing identifier for the segment.
	path       string           // Absolute file path where segment data is stored.
	file       *os.File         // Operating system file handle for I/O operations.
	writer     *bufio.Writer    // Buffered writer to optimize write performance.
	bufferPool *pool.BufferPool // Buffer pool for better memory management.

	// Position tracking for data management and recovery.
	size            uint32    // Total segment size.
	nextLogSequence uint64    // Log Structured Number.
	createdAt       time.Time // Segment creation time.
	currentOffset   uint64    // Current position where next write will occur.
	totalEntries    uint64    // TotalEntries tracks cumulative entries including deleted ones.
	previousOffset  uint64    // Last successfully written data offset, used for recovery and validation.

	// Callback function invoked when segment rotation occurs.
	onRotate func(segment *Segment)

	// Indicates if segment is closed for writing.
	closed atomic.Bool

	// Concurrency control mechanisms.
	wg     sync.WaitGroup     // Tracks completion of background tasks.
	cancel context.CancelFunc // Function to trigger graceful shutdown.
	ctx    context.Context    // Context for canceling background operations.
}

// SegmentInfo holds the metadata and statistics about a storage segment.
type SegmentInfo struct {
	// Unique identifier for this segment in the storage system.
	// Used to order and reference segments during operations.
	SegmentId uint64

	// Total size of segment file in bytes.
	// Used for enforcing size limits and storage management.
	Size int64

	// Number of valid entries currently in the segment.
	// Updated when entries are added or removed.
	Entries uint64

	// Timestamp when segment was initially created.
	// Used for tracking segment lifetime and maintenance.
	CreatedAt time.Time

	// Current byte offset where next write will occur.
	// Maintained to append new entries efficiently.
	CurrentOffset uint64

	// Last successfully written data offset, used for recovery and validation.
	PreviousOffset uint64

	// Next sequence number to be assigned.
	// Ensures strict ordering of entries within segment.
	NextSequenceId uint64

	// Absolute path to segment file on disk.
	// Used for file operations and segment location.
	FilePath string

	// Operating system level file metadata.
	// Contains essential file attributes and state.
	FileMetadata FileMetadata
}

// FileMetadata contains core file system attributes and state.
// Based on standard os.FileInfo interface but optimized for
// frequent access patterns in segment operations.
type FileMetadata struct {
	// Base name of the file without path components.
	// Extracted from full file path for easy reference.
	Name string

	// Indicates if entry represents a directory.
	// Should always be false for segment files.
	IsDir bool

	// Indicates if this is a regular file.
	// Should always be true for segment files.
	IsRegular bool

	// String representation of file permissions.
	// Format example: "-rw-r--r--" for a readable file.
	ModeString string

	// Last modification timestamp of the file.
	// Updated by filesystem on every write operation.
	ModTime time.Time
}

// Record represents a single unit of data in the segment with its associated type.
// It encapsulates both the raw data (Payload) and metadata about the data (Type).
type Record struct {
	// Payload contains the raw binary data to be written.
	Payload []byte

	// Type indicates the semantic meaning and purpose of this record.
	// This determines how the Payload should be interpreted and processed.
	Type domain.EntryType
}

// Config holds the configuration parameters for creating a new segment.
type Config struct {
	// SegmentId is the unique identifier for this segment.
	// Each segment in a WAL must have a unique, monotonically increasing ID.
	SegmentId uint64

	// Byte offset of the last written entry in the segment.
	// Used for appending new entries and reading from specific positions.
	// Should be 0 for new segments.
	LastOffset uint64

	// NextLogSequence represents the next available sequence number for log entries.
	NextLogSequence uint64

	// TotalSizeInBytes represents the current total size of entries in the segment.
	TotalSizeInBytes uint64

	// Options contains WAL-specific configuration parameters.
	Options *domain.WALOptions
}
