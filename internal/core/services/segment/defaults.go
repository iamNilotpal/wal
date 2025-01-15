package segment

import (
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

const (
	DefaultSegmentPrefix    = "segment-"
	DefaultSegmentDirectory = "/segments"

	DefaultMaxSegmentAge  = time.Duration(time.Hour * 24) // 24h
	DefaultMinSegmentSize = 1048576                       // 1MB
	DefaultMaxSegmentSize = 67108864                      // 64MB
)

// DefaultOptions returns a SegmentOptions struct with recommended defaults.
// These defaults are chosen to provide a good balance between performance,
// durability, and resource usage for typical use cases.
func DefaultOptions() *domain.SegmentOptions {
	return &domain.SegmentOptions{
		MaxSegmentAge:    DefaultMaxSegmentAge,
		SegmentPrefix:    DefaultSegmentPrefix,
		MinSegmentSize:   DefaultMinSegmentSize,
		MaxSegmentSize:   DefaultMaxSegmentSize,
		SegmentDirectory: DefaultSegmentDirectory,
	}
}
