// Package domain defines the core types and configurations for the WAL system.
package domain

import (
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain/config"
)

// WALOptions defines the configuration parameters for the Write-Ahead Log system.
// It provides control over storage, performance, durability, and maintenance aspects.
type WALOptions struct {
	// Directory specifies the base path where WAL files will be stored.
	// If empty, the current working directory will be used.
	// The directory must be writable and should be on a durable storage device.
	//
	// Default: "logs"
	Directory string `json:"directory"`

	// BufferSize controls the size of the in-memory write buffer.
	// Larger buffers improve write performance but increase memory usage
	// and potential data loss in case of crashes. Must be between 4KB and 16MB.
	// Should be tuned based on average transaction size and available memory.
	//
	// Default: 1MB
	BufferSize uint32 `json:"bufferSize"`

	// SyncOnWrite determines whether to force sync to disk after each write.
	// Enabling this provides better durability but may impact performance.
	// Recommended for critical data where durability is more important than speed.
	//
	// Default: false
	SyncOnWrite bool `json:"syncOnWrite"`

	// RetentionDays specifies how many days to keep rotated WAL files.
	// Files older than this will be automatically deleted during maintenance.
	// Must be between 1 and 365.
	//
	// Default: 7d
	RetentionDays uint16 `json:"retentionDays"`

	// MinSegmentsKept sets the minimum number of segments to retain,
	// regardless of their age or size. This ensures a minimum history
	// is always available for recovery.
	//
	// Default: 2
	MinSegmentsKept uint16 `json:"minSegmentsKept"`

	// MaxSegmentsKept sets the maximum number of segments to retain.
	// Older segments exceeding this limit will be removed during cleanup.
	// Should be greater than MinSegmentsKept.
	//
	// Default: 10
	MaxSegmentsKept uint16 `json:"maxSegmentsKept"`

	// CleanupInterval defines how often the cleanup process runs to remove
	// old segments exceeding retention limits. More frequent cleanup
	// means more consistent space usage but higher overhead.
	//
	// Default: 15m
	CleanupInterval time.Duration `json:"cleanupInterval"`

	// CompactInterval defines how often the compaction process runs to
	// merge smaller segments. More frequent compaction means more
	// optimal storage but higher overhead.
	//
	// Default: 1h
	CompactInterval time.Duration `json:"compactInterval"`

	// FlushInterval determines how often buffered data is written to disk.
	// Shorter intervals reduce data loss risk but impact performance.
	// Longer intervals improve performance but increase risk of data loss.
	//
	// Default: 5s
	FlushInterval time.Duration `json:"flushInterval"`

	// WriteTimeout specifies the maximum duration allowed for a single write
	// operation to complete. If a write takes longer than this timeout, it will
	// be aborted and return an error. Helps prevent system hangs from slow writes
	// or I/O issues.
	//
	// Should be set based on storage system performance characteristics and
	// expected write sizes. Too low may cause spurious timeouts under load.
	// Too high may delay error detection for failing storage.
	//
	// Default: 1s
	WriteTimeout time.Duration `json:"writeTimeout"`

	// ChecksumOptions configures data integrity verification including
	// algorithm selection, verification frequency and error handling.
	ChecksumOptions *ChecksumOptions `json:"checksumOptions"`

	// CompressionOptions defines data compression settings including
	// algorithm, compression level and entry size thresholds.
	CompressionOptions *CompressionOptions `json:"compressionOptions"`

	// SegmentOptions configures WAL segment management including
	// size limits, naming convention and rotation policies.
	SegmentOptions *SegmentOptions `json:"segmentOptions"`

	// PayloadOptions controls entry size management including
	// validation, optimization and size categories.
	PayloadOptions *config.PayloadOptions `json:"payloadOptions"`
}
