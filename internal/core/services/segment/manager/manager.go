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
	mu     sync.RWMutex       // Guards segment state modifications.
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
func NewSegmentManager(ctx context.Context, opts *domain.WALOptions) (*SegmentManager, error) {
	fs := fs.NewLocalFileSystem()
	ctx, cancel := context.WithCancel(ctx)

	sm := SegmentManager{
		fs:            fs,
		ctx:           ctx,
		opts:          opts,
		cancel:        cancel,
		cleanupTicker: time.NewTicker(opts.CleanupInterval),
		compactTicker: time.NewTicker(opts.CompactInterval),
	}

	path := filepath.Join(opts.Directory, opts.SegmentOptions.SegmentDirectory)
	if err := sm.fs.CreateDir(path, 0755, true); err != nil {
		return nil, err
	}

	segmentId, err := sm.getLatestSegmentId()
	if err != nil {
		cancel()
		return nil, err
	}

	sequenceId, totalEntries, err := sm.scanSegmentMetadata(segmentId)
	if err != nil {
		cancel()
		return nil, err
	}

	if err := sm.loadOrCreateSegment(segmentId, sequenceId, totalEntries); err != nil {
		return nil, err
	}

	return &sm, nil
}

func (sm *SegmentManager) Write(context context.Context, data []byte) error {
	return sm.segment.Write(context, nil)
}

func (sm *SegmentManager) Flush(sync bool) error {
	return sm.segment.Flush(sync)
}

// Returns information about the currently active segment.
func (sm *SegmentManager) SegmentInfo() (*segment.SegmentInfo, error) {
	return sm.segment.Info()
}

// Creates a new segment for writing.
//   - Generates new segment ID by incrementing current
//   - Initializes new segment with zero entries
func (sm *SegmentManager) CreateSegment() (*segment.Segment, error) {
	sm.mu.Lock()
	id := sm.segment.ID() + 1
	sm.mu.Unlock()

	newSeg, err := segment.NewSegment(sm.ctx, id, 0, 0, sm.opts)
	if err != nil {
		return nil, err
	}

	return newSeg, nil
}

// Switches the current active segment with a new one.
//   - Finalizes current segment by writing end metadata
//   - Closes current segment file handles
//   - Updates active segment reference
//
// Returns error if:
//   - Current segment finalization fails
//   - Current segment closure fails
//   - Any file system operations fail
func (sm *SegmentManager) SwitchActiveSegment(segment *segment.Segment) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err := sm.segment.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize active segment : %w", err)
	}

	if err := sm.segment.Close(); err != nil {
		return fmt.Errorf("failed to close active segment : %w", err)
	}

	sm.segment = segment
	return nil
}

// Rotate performs a safe transition from the current segment to a new one.
//
// The rotation process ensures:
//   - No entries are lost during transition
//   - Sequence numbers remain continuous across segments
//   - Clear markers exist for segment boundaries
//   - Proper cleanup of resources
//
// Returns error if any step fails:
//   - Writing rotation entry fails
//   - Finalizing current segment fails
//   - Closing current segment fails
//   - Creating new segment fails
//
// The new segment becomes active only if all steps succeed.
func (sm *SegmentManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := sm.segment.ID()
	lsn := sm.segment.NextLogSequence()

	entry := &domain.Entry{
		Payload: []byte(fmt.Sprintf("rotate-%d", id)),
		Header: &domain.EntryHeader{
			Sequence:  lsn,
			Type:      domain.EntryRotation,
			Timestamp: time.Now().UnixNano(),
		},
	}

	if err := sm.segment.Write(sm.ctx, entry); err != nil {
		return fmt.Errorf("failed to write rotation entry : %w", err)
	}

	newSeg, err := sm.CreateSegment()
	if err != nil {
		return err
	}

	return sm.SwitchActiveSegment(newSeg)
}

// Performs a clean shutdown of the SegmentManager.
func (sm *SegmentManager) Close() error {
	sm.cancel()
	sm.wg.Wait()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.cleanupTicker.Stop()
	sm.compactTicker.Stop()

	return sm.segment.Close()
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
		strId := strings.Split(strings.TrimPrefix(segment, sm.opts.SegmentOptions.SegmentPrefix), ".")[0]

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

// Scans a segment file to find the highest sequence number and total entries.
func (sm *SegmentManager) scanSegmentMetadata(id uint64) (uint64, uint64, error) {
	fileName := fmt.Sprintf("%s%d.log", sm.opts.SegmentOptions.SegmentPrefix, id)
	path := filepath.Join(sm.opts.Directory, sm.opts.SegmentOptions.SegmentDirectory, fileName)

	var offset uint64
	var totalEntries uint64
	var latestSequence uint64

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return latestSequence, totalEntries, fmt.Errorf("failed to open segment %d : %w", id, err)
	}
	defer file.Close()

	for {
		var header domain.EntryHeader
		if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, 0, fmt.Errorf("error reading header at offset %d: %w", offset, err)
		}

		totalEntries++
		if header.Sequence > latestSequence {
			latestSequence = header.Sequence
		}

		offset += uint64(binary.Size(header) + int(header.PayloadSize))
		if _, err := file.Seek(int64(offset), 0); err != nil {
			return 0, 0, fmt.Errorf("error seeking to next entry: %w at offset %d", err, offset)
		}
	}

	return latestSequence, totalEntries, nil
}

// Initializes a new segment with the given ID and sets it as the active segment.
// This ensures there is always a valid segment available for writing new entries.
//
// Returns an error if segment creation fails.
func (sm *SegmentManager) loadOrCreateSegment(id, lsn, total uint64) error {
	segment, err := segment.NewSegment(sm.ctx, id, lsn, total, sm.opts)
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
		sm.opts.Directory, sm.opts.SegmentOptions.SegmentDirectory, sm.opts.SegmentOptions.SegmentPrefix+"*",
	)

	files, err := sm.fs.ReadDir(path)
	if err != nil {
		return []string{}, err
	}

	return files, nil
}
