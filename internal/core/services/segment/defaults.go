package segment

import (
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

const (
	SegmentPrefix    = "segment-"
	SegmentDirectory = "/segments"

	MaxSegmentAge  = time.Duration(time.Hour * 24) // 24h
	MinSegmentSize = 1024 * 1024 * 1               // 1MB
	MaxSegmentSize = 1024 * 1024 * 64              // 64MB
	HeaderSize     = 8 + 4 + 1 + 1                 // 14Bytes

	MinBufferAvailablePercent = 10
	MinWriteTimeout           = 1 * time.Second
	MaxWriteTimeout           = 10 * time.Second
)

// DefaultOptions returns a SegmentOptions struct with recommended defaults.
// These defaults are chosen to provide a good balance between performance,
// durability, and resource usage for typical use cases.
func DefaultOptions() *domain.SegmentOptions {
	return &domain.SegmentOptions{
		MaxSegmentAge:  MaxSegmentAge,
		Prefix:         SegmentPrefix,
		MinSegmentSize: MinSegmentSize,
		MaxSegmentSize: MaxSegmentSize,
		Directory:      SegmentDirectory,
	}
}
