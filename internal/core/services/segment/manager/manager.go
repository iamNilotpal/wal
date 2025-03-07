package sm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/fs"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
	segment "github.com/iamNilotpal/wal/internal/core/services/segment/service"
	"github.com/iamNilotpal/wal/pkg/system"
)

// SegmentManager handles the lifecycle of log segments, including creation,
// rotation, compaction, and cleanup. It coordinates concurrent access and
// background maintenance tasks.
type SegmentManager struct {
	// Configuration options controlling segment behavior, retention,
	// and maintenance schedules.
	opts *domain.WALOptions

	// Interface for file system operations, abstracted for testing.
	fs ports.FileSystemPort

	// Segment state tracking
	segment *segment.Segment // Currently active segment for writing.

	// Concurrency control
	mu     sync.Mutex         // Guards segment state modifications.
	wg     sync.WaitGroup     // Tracks completion of background tasks.
	cancel context.CancelFunc // Function to trigger graceful shutdown.
	ctx    context.Context    // Context for canceling background operations.

	// Background maintenance scheduling
	compactTicker *time.Ticker // Triggers periodic segment compaction operations
	cleanupTicker *time.Ticker // Triggers periodic segment cleanup operations
}

// Creates and initializes a SegmentManager that handles the lifecycle
// of WAL (Write-Ahead Log) segments. It performs the following initialization:
//  1. Sets up filesystem access for segment storage
//  2. Initializes background maintenance timers for cleanup and compaction
//  3. Ensures the segment directory exists with correct permissions
//  4. Discovers or creates the active segment for writing
//
// Returns an error if:
//   - Directory creation fails.
//   - Cannot determine the latest segment ID.
//   - Loading/creating the active segment fails.
func New(ctx context.Context, opts *domain.WALOptions) (*SegmentManager, error) {
	sm := &SegmentManager{}

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				fs := fs.NewLocalFileSystem()
				ctx, cancel := context.WithCancel(ctx)

				sm.fs = fs
				sm.ctx = ctx
				sm.opts = opts
				sm.cancel = cancel
				sm.cleanupTicker = time.NewTicker(opts.CleanupInterval)
				sm.compactTicker = time.NewTicker(opts.CompactInterval)

				// Ensure segment directory exists with proper permissions (0755)
				// Directory is created recursively if it doesn't exist.
				path := filepath.Join(opts.Directory, opts.SegmentOptions.Directory)
				if err := sm.fs.CreateDir(path, 0755, true); err != nil {
					cancel()
					return err
				}

				// Find the highest segment ID from existing segment files
				// This ensures we continue from the last valid segment.
				segmentId, err := sm.getLatestSegmentId()
				if err != nil {
					cancel()
					return err
				}

				// Scan the latest segment to determine the next sequence ID
				// and total number of entries for continuity.
				sequenceId, totalEntries, offset, err := sm.scanSegmentMetadata(segmentId)
				if err != nil {
					cancel()
					return err
				}

				// Either load existing segment or create new one based on discovered metadata.
				if err := sm.loadOrCreateSegment(segmentId, sequenceId, totalEntries, offset); err != nil {
					return err
				}

				// Registers a callback on the active segment to maintain segment
				// references when rotation occurs. This ensures the SegmentManager
				// always points to the most recent active segment.
				sm.segment.RegisterRotationHandler(func(segment *segment.Segment) {
					sm.mu.Lock()
					sm.segment = segment
					sm.mu.Unlock()
				})

				return nil
			}
		}
	}); err != nil {
		return nil, err
	}

	return sm, nil
}

// ReadAt reads an entry from the segment at the specified offset.
// This method delegates the read operation to the underlying segment, ensuring
// proper data retrieval while maintaining abstraction at the SegmentManager level.
func (sm *SegmentManager) ReadAt(ctx context.Context, offset int64) (*domain.Entry, error) {
	return sm.segment.ReadAt(ctx, offset)
}

// ReadAll reads all entries from the segment.
// This method delegates the read operation to the underlying segment, ensuring
// proper data retrieval while maintaining abstraction at the SegmentManager level.
func (sm *SegmentManager) ReadAll(ctx context.Context) ([]*domain.Entry, error) {
	return sm.segment.ReadAll(ctx)
}

// Write creates a new record with the provided data and writes it to the current segment.
// It wraps the raw data in a Record structure with a normal entry type before writing.
func (sm *SegmentManager) Write(context context.Context, data []byte, sync bool) error {
	entry := segment.Record{Payload: data, Type: domain.EntryNormal}
	return sm.segment.Write(context, &entry, sync)
}

// Flush ensures all buffered data in the current segment is written to stable storage.
func (sm *SegmentManager) Flush(context context.Context) error {
	return sm.segment.Flush(context)
}

// Rotate performs a safe transition from the current segment to a new one.
func (sm *SegmentManager) Rotate(context context.Context) error {
	segment, err := sm.segment.Rotate(context)
	if err != nil {
		return err
	}

	sm.segment = segment
	return nil
}

// Returns information about the currently active segment.
func (sm *SegmentManager) SegmentInfo() (*segment.SegmentInfo, error) {
	return sm.segment.Info()
}

// Creates a new segment for writing.
//   - Generates new segment ID by incrementing current
//   - Initializes new segment with zero entries
func (sm *SegmentManager) CreateSegment(context context.Context) (*segment.Segment, error) {
	id, err := sm.segment.ID()
	if err != nil {
		return nil, err
	}

	newSegment, err := segment.New(
		context,
		&segment.Config{
			NextLogSequence:  0,
			TotalSizeInBytes: 0,
			SegmentId:        id + 1,
			Options:          sm.opts,
		},
	)
	if err != nil {
		return nil, err
	}

	return newSegment, nil
}

// Switches the current active segment with a new one.
func (sm *SegmentManager) SwitchActiveSegment(context context.Context, segment *segment.Segment) error {
	if err := sm.segment.Finalize(context); err != nil {
		return fmt.Errorf("failed to finalize active segment : %w", err)
	}

	if err := sm.segment.Close(context); err != nil {
		return fmt.Errorf("failed to close active segment : %w", err)
	}

	sm.segment = segment
	return nil
}

// Performs a clean shutdown of the SegmentManager.
func (sm *SegmentManager) Close(context context.Context) error {
	sm.cancel()
	sm.wg.Wait()

	sm.cleanupTicker.Stop()
	sm.compactTicker.Stop()

	return sm.segment.Close(context)
}

// Scans the segment directory to determine the highest segment ID currently in use.
//  1. Lists all files matching the segment prefix pattern
//  2. Parses segment IDs from filenames (format: prefix + number + extension)
//  3. Returns the highest ID found, or 0 if no segments exist
//
// Returns an error if:
//   - Directory reading fails
//   - Any segment filename cannot be parsed
func (sm *SegmentManager) getLatestSegmentId() (uint64, error) {
	var latestId uint64 = 0

	files, err := sm.loadExistingSegments()
	if err != nil {
		return latestId, fmt.Errorf("error loading latest segment id : %w", err)
	}

	for _, name := range files {
		_, segment := filepath.Split(name)
		strId := strings.Split(strings.TrimPrefix(segment, sm.opts.SegmentOptions.Prefix), ".")[0]

		id, err := strconv.Atoi(strId)
		if err != nil {
			return 0, err
		}

		segmentId := uint64(id)
		if segmentId > latestId {
			latestId = segmentId
		}
	}

	return latestId, nil
}

// Scans a segment file to find the highest sequence number, total entries and last offset.
func (sm *SegmentManager) scanSegmentMetadata(id uint64) (uint64, uint64, uint64, error) {
	fileName := fmt.Sprintf("%s%d.log", sm.opts.SegmentOptions.Prefix, id)
	path := filepath.Join(sm.opts.Directory, sm.opts.SegmentOptions.Directory, fileName)

	var offset uint64
	var totalEntries uint64
	var latestSequence uint64

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, nil
		}
		return latestSequence, totalEntries, offset, fmt.Errorf("failed to open segment %d : %w", id, err)
	}
	defer file.Close()

	for {
		var header domain.EntryHeader
		if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, 0, 0, fmt.Errorf("error reading header at offset %d: %w", offset, err)
		}

		totalEntries++
		if header.Sequence > latestSequence {
			latestSequence = header.Sequence
		}

		offset += uint64(binary.Size(header) + int(header.PayloadSize))
		if _, err := file.Seek(int64(offset), 0); err != nil {
			return 0, 0, 0, fmt.Errorf("error seeking to next entry: %w at offset %d", err, offset)
		}
	}

	latestSequence += 1
	return latestSequence, totalEntries, offset, nil
}

// Initializes a new segment with the given ID and sets it as the active segment.
// This ensures there is always a valid segment available for writing new entries.
//
// Returns an error if segment creation fails.
func (sm *SegmentManager) loadOrCreateSegment(id, lsn, total, offset uint64) error {
	segment, err := segment.New(
		context.Background(),
		&segment.Config{
			SegmentId:        id,
			NextLogSequence:  lsn,
			TotalSizeInBytes: total,
			LastOffset:       offset,
			Options:          sm.opts,
		},
	)
	if err != nil {
		return err
	}

	sm.segment = segment
	return nil
}

// Returns a list of all segment files in the configured directory that
// match the segment prefix pattern. Used during initialization and segment
// maintenance operations.
//
// Returns an empty slice and error if directory reading fails.
func (sm *SegmentManager) loadExistingSegments() ([]string, error) {
	path := filepath.Join(
		sm.opts.Directory, sm.opts.SegmentOptions.Directory, sm.opts.SegmentOptions.Prefix+"*",
	)

	files, err := sm.fs.ReadDir(path)
	if err != nil {
		return []string{}, err
	}

	return files, nil
}
