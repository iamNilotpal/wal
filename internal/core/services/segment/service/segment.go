package segment

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
)

// Segment represents a single log file on disk that stores sequential data.
// It provides thread-safe operations for writing log entries and managing segment state.
type Segment struct {
	// Configuration options for the WAL system, including segment behavior,
	// compression settings, and checksum parameters.
	opts *domain.WALOptions

	// Interfaces for data integrity and compression operations.
	checksum   ports.ChecksumPort    // Handles data integrity verification.
	compressor ports.CompressionPort // Handles data compression/decompression.

	// Core segment properties
	id     uint64        // Unique monotonically increasing identifier for the segment.
	path   string        // Absolute file path where segment data is stored.
	size   uint64        // Current size of segment file in bytes.
	file   *os.File      // Operating system file handle for I/O operations.
	writer *bufio.Writer // Buffered writer to optimize write performance.

	// Position tracking for data management and recovery.
	currentOffset uint64 // Current position where next write will occur.
	lastOffset    uint64 // Position of last successful write, used for crash recovery.
	firstSeq      uint64 // First sequence number in this segment, for ordering.
	lastSeq       uint64 // Last sequence number in this segment, for ordering.

	// State management flags.
	corrupt atomic.Bool // Indicates if segment has detected data corruption.
	closed  atomic.Bool // Indicates if segment is closed for writing.

	// Concurrency control mechanisms
	cancel  context.CancelFunc // Function to trigger graceful shutdown.
	ctx     context.Context    // Context for canceling background operations.
	mu      sync.RWMutex       // Protects concurrent access to segment metadata.
	flushMu sync.Mutex         // Serializes flush operations to prevent data races.
}

func NewSegment(ctx context.Context, id uint64, opts *domain.WALOptions) (*Segment, error) {
	path := filepath.Join(opts.Directory, opts.SegmentOptions.SegmentDirectory)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("error creating segment file : %w", err)
	}

	stats, err := file.Stat()
	if err != nil {
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("error closing file : %w", err)
		}
		return nil, fmt.Errorf("error getting file stats : %w", err)
	}

	size := uint64(stats.Size())
	ctx, cancel := context.WithCancel(ctx)

	segment := Segment{
		id:     id,
		ctx:    ctx,
		path:   path,
		file:   file,
		size:   size,
		opts:   opts,
		cancel: cancel,
		writer: bufio.NewWriterSize(file, int(opts.BufferSize)),
	}

	if opts.ChecksumOptions.Enable {
		if opts.ChecksumOptions.Custom != nil {
			segment.checksum = opts.ChecksumOptions.Custom
		} else {
			segment.checksum = checksum.NewCheckSummer(opts.ChecksumOptions.Algorithm)
		}
	}

	if opts.CompressionOptions.Enable {
		segment.compressor, err = compression.NewZstdCompression(
			compression.Options{
				Level:              opts.CompressionOptions.Level,
				EncoderConcurrency: opts.CompressionOptions.EncoderConcurrency,
				DecoderConcurrency: opts.CompressionOptions.DecoderConcurrency,
			},
		)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error creating compressor : %w", err)
		}
	}

	return &segment, nil
}
