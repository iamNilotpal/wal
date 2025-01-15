package sm

import (
	"context"
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
	nextSegmentID uint64           // Monotonically increasing ID for new segments.

	// Concurrency control
	mu     sync.RWMutex       // Guards segment state modifications.
	wg     sync.WaitGroup     // Tracks completion of background tasks.
	cancel context.CancelFunc // Function to trigger graceful shutdown.
	ctx    context.Context    // Context for canceling background operations.

	// Background maintenance scheduling
	compactTicker *time.Ticker // Triggers periodic segment compaction operations
	cleanupTicker *time.Ticker // Triggers periodic segment cleanup operations
}

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

	return &sm, nil
}
