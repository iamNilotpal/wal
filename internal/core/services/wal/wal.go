package wal

import (
	"context"

	"github.com/iamNilotpal/wal/internal/core/domain"
	sm "github.com/iamNilotpal/wal/internal/core/services/segment/manager"
	segment "github.com/iamNilotpal/wal/internal/core/services/segment/service"
)

// WAL implements a Write-Ahead Log for durability and crash recovery.
// It provides ordered, durable storage of log entries by writing them to disk
// before acknowledging writes. The WAL ensures data consistency through
// sequential logging and supports crash recovery.
type WAL struct {
	// Core components and configuration
	options *domain.WALOptions // Configuration controlling WAL behavior
	sm      *sm.SegmentManager // Handles segment lifecycle and maintenance

	// Lifecycle and concurrency control
	ctx    context.Context    // Controls the WAL's operational lifecycle
	cancel context.CancelFunc // Triggers graceful shutdown of WAL operations
}

// Creates a new Write-Ahead Log (WAL) instance with the provided options.
// If no options are provided, default values will be used.
func New(opts *domain.WALOptions) (*WAL, error) {
	// Validate options if provided
	if opts != nil {
		if err := Validate(opts); err != nil {
			return nil, err
		}
	}

	// Set default options if not provided or merge defaults with provided options
	if opts != nil {
		opts = prepareDefaults(opts)
	} else {
		opts = prepareDefaults(&domain.WALOptions{})
	}

	// Create a cancellable context for managing WAL lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize the segment manager which handles the underlying segments
	sm, err := sm.NewSegmentManager(ctx, opts)
	if err != nil {
		cancel() // Clean up context if segment manager creation fails
		return nil, err
	}

	return &WAL{
		sm:      sm,
		ctx:     ctx,
		options: opts,
		cancel:  cancel,
	}, nil
}

// Writes an entry containing the provided data bytes to the active segment. The context
// allows cancellation of long-running writes. Each write is atomic and sequential,
// maintaining the WAL's ordering guarantees.
//
// It syncs to disk if either:
//   - sync parameter is true.
//   - sync is false but SyncOnWrite is true.
func (wal *WAL) Write(context context.Context, data []byte, sync bool) error {
	return wal.sm.Write(context, data, sync)
}

// Rotates to a new segment when the current one reaches capacity. This prevents any single
// segment from growing too large while maintaining write availability.
func (wal *WAL) Rotate() error {
	return wal.sm.Rotate()
}

// Ensures durability of written entries by flushing buffers to disk. The sync flag controls
// whether to force a file sync.
//
// It syncs to disk if either:
//   - sync parameter is true.
//   - sync is false but SyncOnFlush is true.
func (wal *WAL) Flush(sync bool) error {
	return wal.sm.Flush(sync)
}

// Creates a new segment which is a single file where WAL entries could be written.
func (wal *WAL) CreateSegment() (*segment.Segment, error) {
	return wal.sm.CreateSegment()
}

// Switches the target segment for new writes. This allows external control over which
// segment receives entries, enabling operations like log compaction and segment retirement.
func (wal *WAL) SwitchActiveSegment(segment *segment.Segment) error {
	return wal.sm.SwitchActiveSegment(segment)
}

// Retrieves metadata about the active segment including its current size, number of entries,
// and file details. This information helps track segment growth and determine when rotation
// is needed.
func (wal *WAL) SegmentInfo() (*segment.SegmentInfo, error) {
	return wal.sm.SegmentInfo()
}

// Gracefully shuts down the WAL by cancelling ongoing operations and ensuring all segments
// are properly closed and synced to disk. This prevents data corruption from improper shutdown.
func (wal *WAL) Close() error {
	wal.cancel()
	return wal.sm.Close()
}
