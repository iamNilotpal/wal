package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iamNilotpal/wal/internal/core/domain"
	sm "github.com/iamNilotpal/wal/internal/core/services/segment/manager"
	segment "github.com/iamNilotpal/wal/internal/core/services/segment/service"
	"github.com/iamNilotpal/wal/pkg/errors"
	"github.com/iamNilotpal/wal/pkg/system"
)

// WAL implements a Write-Ahead Log for durability and crash recovery.
// It provides ordered, durable storage of log entries by writing them to disk
// before acknowledging writes. The WAL ensures data consistency through
// sequential logging and supports crash recovery.
type WAL struct {
	// Core components and configuration
	options *domain.WALOptions // Configuration controlling WAL behavior.
	sm      *sm.SegmentManager // Handles segment lifecycle and maintenance.
}

// Creates a new Write-Ahead Log (WAL) instance with the provided options.
// If no options are provided, default values will be used.
func New(ctx context.Context, opts *domain.WALOptions) (*WAL, error) {
	var wal *WAL

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				// Validate options if provided
				if opts != nil {
					if err := Validate(opts); err != nil {
						return err
					}
				}

				// Set default options if not provided or merge defaults with provided options
				if opts != nil {
					opts = prepareDefaults(opts)
				} else {
					opts = prepareDefaults(&domain.WALOptions{})
				}

				// Initialize the segment manager which handles the underlying segments
				sm, err := sm.NewSegmentManager(ctx, opts)
				if err != nil {
					return err
				}

				wal.sm = sm
				wal.options = opts
				path := filepath.Join(opts.Directory, "meta.json")

				_, err = os.Stat(path)
				if err != nil {
					return fmt.Errorf("error in getting file stat %s because of %v", path, err)
				}

				metaFile, err := os.Create(path)
				if err != nil {
					if err := wal.Close(ctx); err != nil {
						return err
					}
					return fmt.Errorf("error creating meta.json file : %w", err)
				}

				data, err := json.Marshal(opts)
				if err != nil {
					if err := wal.Close(ctx); err != nil {
						return err
					}
					return fmt.Errorf("error marshalling options : %w", err)
				}

				if n, err := metaFile.Write(data); err != nil {
					if err := wal.Close(ctx); err != nil {
						return err
					}
					return fmt.Errorf("failed to write meta.json file : %w", err)
				} else if n < len(data) {
					if err := wal.Close(ctx); err != nil {
						return err
					}
					return fmt.Errorf("failed to write meta.json file : %w", err)
				}

				return nil
			}
		}
	}); err != nil {
		return nil, err
	}

	return wal, nil
}

// ReadAt retrieves an entry from the Write-Ahead Log (WAL) at the specified offset.
// This method reads an entry stored in the WAL at the given byte offset. It ensures
// data integrity by verifying the read operation and handling potential failures.
//
// Parameters:
//   - ctx: Context for managing request lifecycle, supporting cancellation and timeouts.
//   - offset: The byte offset in the WAL from which to read the entry.
//
// Returns:
//   - *domain.Entry: A pointer to the retrieved entry if the operation is successful.
//   - error: Returns an error if fails.
func (wal *WAL) ReadAt(ctx context.Context, offset int64) (*domain.Entry, error) {
	return wal.sm.ReadAt(ctx, offset)
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
	return wal.sm.Close(context)
}

func (wal *WAL) validateSize(size uint32) error {
	if size < wal.options.PayloadOptions.MinSize {
		return errors.NewValidationError(
			"payloadSize",
			size,
			fmt.Errorf("invalid payload size, expected %d got %d", wal.options.PayloadOptions.MinSize, size),
		)
	}

	if size > wal.options.PayloadOptions.MaxSize {
		return errors.NewValidationError(
			"payloadSize",
			size,
			fmt.Errorf("invalid payload size, expected %d got %d", wal.options.PayloadOptions.MaxSize, size),
		)
	}

	return nil
}
