// Package segment provides configuration options for WAL (Write-Ahead Log) segment management.
package domain

import (
	"time"
)

// SegmentOptions defines configurable parameters for WAL segments.
// It provides fine-grained control over segment behavior, performance, and resource utilization.
type SegmentOptions struct {
	// MaxSegmentSize defines the maximum size a segment can grow to before rotation.
	// When a segment reaches this size, a new segment will be created.
	// Larger segments mean fewer files but slower compaction and recovery.
	// Must be at least 1MB. Default is 64MB if set to 0.
	MaxSegmentSize uint32

	// MinSegmentSize defines the minimum size a segment should reach before
	// allowing rotation. This prevents creating too many small segments.
	// Should be significantly smaller than MaxSegmentSize.
	//
	// Default: 1MB
	MinSegmentSize uint32

	// MaxSegmentAge defines the maximum time a segment should remain active
	// before rotation, regardless of size. This ensures regular rotation
	// even with low write volume.
	//
	// Default: 24 hours
	MaxSegmentAge time.Duration

	// SegmentDirectory specifies where segment files are stored.
	// Should be on a filesystem appropriate for write-ahead logging.
	//
	// Default: "/segments"
	SegmentDirectory string

	// SegmentPrefix defines the filename prefix for segment files.
	// Final filename will be: prefix + segmentID + extension
	//
	// Default: "segment-"
	SegmentPrefix string
}
