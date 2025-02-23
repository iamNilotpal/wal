package segment

import (
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

const (
	SegmentPrefix    = "segment-"
	SegmentDirectory = "/segments"

	MaxSegmentAge  = time.Duration(time.Hour * 24) // 24h
	MinSegmentSize = 1048576                       // 1MB
	MaxSegmentSize = 67108864                      // 64MB
	HeaderSize     = 14                            // 14Bytes

	MinBufferAvailablePercent = 25
	WriteTimeout              = 1 * time.Second
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
