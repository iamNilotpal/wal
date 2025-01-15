package segment

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/adapters/fs"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
)

var (
	// ErrSegmentNotFound indicates requested segment doesn't exist
	ErrSegmentNotFound = errors.New("segment not found")

	// ErrSegmentClosed indicates operation on closed segment
	ErrSegmentClosed = errors.New("segment is closed")
)

// Segment represents a single log file on disk that stores sequential data.
// It provides thread-safe operations for writing log entries and managing segment state.
type Segment struct {
	// Configuration options for the WAL system, including segment behavior,
	// compression settings, and checksum parameters.
	opts *domain.WALOptions

	// Interfaces for data integrity and compression operations.
	fs         ports.FileSystemPort  // Handles File System operations.
	checksum   ports.ChecksumPort    // Handles data integrity verification.
	compressor ports.CompressionPort // Handles data compression/decompression.

	// Core segment properties
	id     uint64        // Unique monotonically increasing identifier for the segment.
	path   string        // Absolute file path where segment data is stored.
	size   uint32        // Current size of segment file in bytes.
	file   *os.File      // Operating system file handle for I/O operations.
	writer *bufio.Writer // Buffered writer to optimize write performance.

	// Position tracking for data management and recovery.
	currentOffset uint64 // Current position where next write will occur.
	lastOffset    uint64 // Position of last successful write, used for crash recovery.
	firstSeq      uint64 // First sequence number in this segment, for ordering.
	lastSeq       uint64 // Last sequence number in this segment, for ordering.

	// State management flags.
	closed  atomic.Bool // Indicates if segment is closed for writing.
	corrupt atomic.Bool // Indicates if segment has detected data corruption.

	// Concurrency control mechanisms
	wg      sync.WaitGroup     // Tracks completion of background tasks.
	cancel  context.CancelFunc // Function to trigger graceful shutdown.
	ctx     context.Context    // Context for canceling background operations.
	mu      sync.RWMutex       // Protects concurrent access to segment metadata.
	flushMu sync.Mutex         // Serializes flush operations to prevent data races.
}

// NewSegment creates a new log segment with the given ID and options.
// It handles file creation, size checks, and initializes necessary components
// like compression and checksum if enabled.
//
// Parameters:
//   - context: Context for cancellation.
//   - id: Unique identifier for the segment.
//   - opts: Configuration options for the WAL (Write-Ahead Log).
//
// Returns:
//   - *Segment: Initialized segment instance.
//   - error: Any error encountered during initialization.
func NewSegment(ctx context.Context, id uint64, opts *domain.WALOptions) (*Segment, error) {
	fs := fs.NewLocalFileSystem()
	segment := Segment{id: id, opts: opts, fs: fs}

	// Generate segment file path
	fileName := segment.generateName()
	path := filepath.Join(opts.Directory, opts.SegmentOptions.SegmentDirectory, fileName)

	// Create/Open segment file with read-write-append permissions.
	// 0644 permissions: owner can read/write, others can only read.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
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

	size := uint32(stats.Size())
	ctx, cancel := context.WithCancel(ctx)

	// If current segment exceeds max size, create new segment with incremented id.
	if size >= segment.opts.SegmentOptions.MaxSegmentSize {
		if err := file.Close(); err != nil {
			cancel()
			return nil, fmt.Errorf("error closing file : %w", err)
		}

		size = 0
		segment.id++

		fileName = segment.generateName()
		path = filepath.Join(opts.Directory, opts.SegmentOptions.SegmentDirectory, fileName)

		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error creating segment file : %w", err)
		}
	}

	// Move file pointer to end for appending.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		cancel()
		return nil, err
	}

	segment.ctx = ctx
	segment.path = path
	segment.file = file
	segment.size = size
	segment.cancel = cancel
	segment.writer = bufio.NewWriterSize(file, int(opts.BufferSize))

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

	// Write the segment file header for new segments (size == 0).
	// The header contains format version, creation timestamp, and segment metadata
	// that must be present before any entries can be written. This distinguishes
	// valid segments from corrupted or incomplete files.
	//
	// Without this header, readers cannot verify segment integrity or parse entries.
	if size == 0 {
		if err := segment.writeEntryHeader(); err != nil {
			cancel()
			if err := segment.file.Close(); err != nil {
				return nil, fmt.Errorf("error closing file : %w", err)
			}
			return nil, err
		}
	}

	return &segment, nil
}

func (s *Segment) ID() uint64 {
	return s.id
}

func (s *Segment) Write(context context.Context, entry *domain.Entry) error {
	return nil
}

func (s *Segment) Flush() error {
	return nil
}

// FinalizeAndFlush marks the segment as closed by writing a final metadata entry
// and ensures all buffered data is written to the underlying storage.
// The method is idempotent and should be called before closing or archiving
// the segment to ensure data durability.
//
// Returns an error if either writing the final entry or flushing fails.
func (s *Segment) FinalizeAndFlush() error {
	entry := domain.Entry{
		Payload: []byte("Final Entry"),
		Header: &domain.EntryHeader{
			Compression: s.compressor.Level(),
			Type:        domain.EntryMetadata,
			Timestamp:   time.Now().UnixNano(),
		},
	}

	entry.Header.PayloadSize = uint32(binary.Size(entry.Payload))
	entry.Header.Checksum = s.checksum.Calculate(entry.Payload)

	if err := s.Write(s.ctx, &entry); err != nil {
		return fmt.Errorf("failed to write final entry : %w", err)
	}

	return s.Flush()
}

// Close safely shuts down the segment and releases all associated resources.
// The method is thread-safe and ensures proper cleanup even under concurrent access.
// Once closed, any subsequent operations on the segment will return ErrSegmentClosed.
//
// Returns an error if any cleanup operation fails. The first error encountered
// during cleanup will be returned, and subsequent cleanup steps will be skipped.
func (s *Segment) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrSegmentClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel()
	s.wg.Wait()

	if err := s.compressor.Close(); err != nil {
		return err
	}

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("error flushing buffer : %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("error flushing file contents : %w", err)
	}

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("error closing file : %w", err)
	}

	return nil
}

// Writes the initial metadata entry at sequence 0 that identifies this segment.
//   - Sequence number 0 (reserved for segment header)
//   - EntryMetadata type to distinguish from data entries
//
// This header entry allows readers to:
//  1. Verify they are reading a valid segment file.
//  2. Track when the segment was created.
//  3. Match the segment file to its logical ID.
//
// An error is returned if writing the header entry fails.
func (s *Segment) writeEntryHeader() error {
	header := domain.Entry{
		Header: &domain.EntryHeader{
			Sequence:  0,
			Type:      domain.EntryMetadata,
			Timestamp: time.Now().UnixNano(),
		},
		Payload: []byte(fmt.Sprintf("segment-%d", s.id)),
	}

	return s.Write(s.ctx, &header)
}

// Creates a segment filename by combining the configured prefix
// with the segment id. For example: "segment-0.log", "segment-1.log", etc.
func (s *Segment) generateName() string {
	return fmt.Sprintf("%s%d.log", s.opts.SegmentOptions.SegmentPrefix, s.id)
}
