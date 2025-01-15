package sm

import (
	"context"
	"fmt"
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
	activeSegment *segment.Segment // Currently active segment for writing.

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

	latestId, err := sm.getLatestSegmentId()
	if err != nil {
		cancel()
		return nil, err
	}

	if err := sm.loadOrCreateSegment(latestId); err != nil {
		return nil, err
	}

	return &sm, nil
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

// Initializes a new segment with the given ID and sets it as the active segment.
// This ensures there is always a valid segment available for writing new entries.
//
// Returns an error if segment creation fails.
func (sm *SegmentManager) loadOrCreateSegment(id uint64) error {
	segment, err := segment.NewSegment(sm.ctx, id, sm.opts)
	if err != nil {
		return err
	}

	sm.activeSegment = segment
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

// Performs a clean shutdown of the SegmentManager.
func (sm *SegmentManager) Close() error {
	sm.cancel()
	sm.wg.Wait()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.cleanupTicker.Stop()
	sm.compactTicker.Stop()

	return sm.activeSegment.Close()
}
