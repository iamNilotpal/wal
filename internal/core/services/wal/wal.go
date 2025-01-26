package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/iamNilotpal/wal/internal/adapters/fs"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
	sm "github.com/iamNilotpal/wal/internal/core/services/segment/manager"
	segment "github.com/iamNilotpal/wal/internal/core/services/segment/service"
	"github.com/iamNilotpal/wal/pkg/errors"
)

// WAL implements a Write-Ahead Log for durability and crash recovery.
// It provides ordered, durable storage of log entries by writing them to disk
// before acknowledging writes. The WAL ensures data consistency through
// sequential logging and supports crash recovery.
type WAL struct {
	// Core components and configuration
	options *domain.WALOptions   // Configuration controlling WAL behavior.
	sm      *sm.SegmentManager   // Handles segment lifecycle and maintenance.
	fs      ports.FileSystemPort // Local file system access.

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

	fs := fs.NewLocalFileSystem()
	metaFile, err := fs.CreateFile(filepath.Join(opts.Directory, "meta.json"), true)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error creating meta.json file : %w", err)
	}

	data, err := json.Marshal(opts)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error marshalling options : %w", err)
	}

	if n, err := metaFile.Write(data); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to write meta.json file : %w", err)
	} else if n < len(data) {
		cancel()
		return nil, fmt.Errorf("failed to write meta.json file : %w", err)
	}

	return &WAL{fs: fs, sm: sm, ctx: ctx, options: opts, cancel: cancel}, nil
}

// Writes an entry containing the provided data bytes to the active segment. The context
// allows cancellation of long-running writes. Each write is atomic and sequential,
// maintaining the WAL's ordering guarantees.
//
// It syncs to disk if either:
//   - sync parameter is true.
//   - sync is false but SyncOnWrite is true.
func (wal *WAL) Write(context context.Context, data []byte, sync bool) error {
	if err := wal.validateSize(uint32(len(data))); err != nil {
		return err
	}
	return wal.sm.Write(context, data, sync)
}

// Rotates to a new segment when the current one reaches capacity. This prevents any single
// segment from growing too large while maintaining write availability.
func (wal *WAL) Rotate(context context.Context) error {
	return wal.sm.Rotate(context)
}

// Ensures durability of written entries by flushing buffers to disk. The sync flag controls
// whether to force a file sync.
//
// It syncs to disk if either:
//   - sync parameter is true.
//   - sync is false but SyncOnFlush is true.
func (wal *WAL) Flush(context context.Context, sync bool) error {
	return wal.sm.Flush(context, sync)
}

// Creates a new segment which is a single file where WAL entries could be written.
func (wal *WAL) CreateSegment(context context.Context) (*segment.Segment, error) {
	return wal.sm.CreateSegment(context)
}

// Switches the target segment for new writes. This allows external control over which
// segment receives entries, enabling operations like log compaction and segment retirement.
func (wal *WAL) SwitchActiveSegment(context context.Context, segment *segment.Segment) error {
	return wal.sm.SwitchActiveSegment(context, segment)
}

// Retrieves metadata about the active segment including its current size, number of entries,
// and file details. This information helps track segment growth and determine when rotation
// is needed.
func (wal *WAL) SegmentInfo() (*segment.SegmentInfo, error) {
	return wal.sm.SegmentInfo()
}

// Gracefully shuts down the WAL by cancelling ongoing operations and ensuring all segments
// are properly closed and synced to disk. This prevents data corruption from improper shutdown.
func (wal *WAL) Close(context context.Context) error {
	wal.cancel()
	return wal.sm.Close(context)
}

func (wal *WAL) validateSize(size uint32) error {
	if size < wal.options.PayloadConfig.MinSize {
		return errors.NewValidationError(
			"PayloadSize", size, fmt.Errorf("invalid payload size, expected %d got %d", wal.options.PayloadConfig.MinSize, size),
		)
	}

	if size > wal.options.PayloadConfig.MaxSize {
		return errors.NewValidationError(
			"PayloadSize", size, fmt.Errorf("invalid payload size, expected %d got %d", wal.options.PayloadConfig.MaxSize, size),
		)
	}

	return nil
}
