package segment

import (
	"bufio"
	"bytes"
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
	file   *os.File      // Operating system file handle for I/O operations.
	writer *bufio.Writer // Buffered writer to optimize write performance.

	// Position tracking for data management and recovery.
	nextLSN      uint64    // Log Structured Number.
	createdAt    time.Time // Segment creation time.
	offset       uint64    // Current position where next write will occur.
	totalEntries uint64    // TotalEntries tracks cumulative entries including deleted ones.

	// State management flags.
	closed atomic.Bool // Indicates if segment is closed for writing.

	// Concurrency control mechanisms
	wg      sync.WaitGroup     // Tracks completion of background tasks.
	cancel  context.CancelFunc // Function to trigger graceful shutdown.
	ctx     context.Context    // Context for canceling background operations.
	mu      sync.RWMutex       // Protects concurrent access to segment metadata.
	flushMu sync.Mutex         // Serializes flush operations to prevent data races.
}

// SegmentInfo holds the metadata and statistics about a storage segment.
type SegmentInfo struct {
	// Unique identifier for this segment in the storage system.
	// Used to order and reference segments during operations.
	SegmentId uint64

	// Total size of segment file in bytes.
	// Used for enforcing size limits and storage management.
	Size int64

	// Number of valid entries currently in the segment.
	// Updated when entries are added or removed.
	Entries uint64

	// Timestamp when segment was initially created.
	// Used for tracking segment lifetime and maintenance.
	CreatedAt time.Time

	// Current byte offset where next write will occur.
	// Maintained to append new entries efficiently.
	CurrentOffset uint64

	// Next sequence number to be assigned.
	// Ensures strict ordering of entries within segment.
	NextSequenceId uint64

	// Absolute path to segment file on disk.
	// Used for file operations and segment location.
	FilePath string

	// Operating system level file metadata.
	// Contains essential file attributes and state.
	FileMetadata FileMetadata
}

// FileMetadata contains core file system attributes and state.
// Based on standard os.FileInfo interface but optimized for
// frequent access patterns in segment operations.
type FileMetadata struct {
	// Base name of the file without path components.
	// Extracted from full file path for easy reference.
	Name string

	// Indicates if entry represents a directory.
	// Should always be false for segment files.
	IsDir bool

	// Indicates if this is a regular file.
	// Should always be true for segment files.
	IsRegular bool

	// String representation of file permissions.
	// Format example: "-rw-r--r--" for a readable file.
	ModeString string

	// Last modification timestamp of the file.
	// Updated by filesystem on every write operation.
	ModTime time.Time
}

// NewSegment creates or opens a new log segment with the given ID and options.
// It performs the following operations:
//   - Creates/opens segment file with read-write-append permissions.
//   - Checks current size against configured max segment size.
//   - If size exceeded, creates new segment with incremented ID.
//   - Sets up buffered writer with configured buffer size.
//   - Initializes checksums if enabled in options.
//   - Initializes compression if enabled in options.
//   - For new segments (size=0), writes initial header.
//
// Parameters:
//   - ctx: Context for managing cancellation.
//   - id: Unique segment identifier.
//   - lsn: Next log sequence number to use.
//   - total: Count of entries if segment already exists.
//   - opts: WAL configuration options.
func NewSegment(ctx context.Context, id, lsn, total uint64, opts *domain.WALOptions) (*Segment, error) {
	fs := fs.NewLocalFileSystem()
	segment := Segment{
		id:           id,
		fs:           fs,
		nextLSN:      lsn,
		opts:         opts,
		totalEntries: total,
		createdAt:    time.Now(),
	}

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
		segment.nextLSN = 0
		segment.totalEntries = 0

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

// Returns the unique identifier of the segment.
func (s *Segment) ID() uint64 {
	return s.id
}

// Returns the next available log sequence number (LSN).
func (s *Segment) NextLogSequence() uint64 {
	return s.nextLSN
}

// Returns metadata about this segment including file information and segment-specific details.
// It combines both filesystem metadata and segment-specific tracking information.
func (s *Segment) Info() (*SegmentInfo, error) {
	stat, err := s.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to load file stats : %w", err)
	}

	info := FileMetadata{
		Name:       stat.Name(),
		IsDir:      stat.IsDir(),
		ModTime:    stat.ModTime(),
		ModeString: stat.Mode().String(),
		IsRegular:  stat.Mode().IsRegular(),
	}

	return &SegmentInfo{
		FileMetadata:   info,
		SegmentId:      s.id,
		FilePath:       s.path,
		CurrentOffset:  s.offset,
		NextSequenceId: s.nextLSN,
		Size:           stat.Size(),
		CreatedAt:      s.createdAt,
		Entries:        s.totalEntries,
	}, nil
}

func (s *Segment) Write(context context.Context, entry *domain.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalEntries++
	return nil
}

// Flush ensures that all buffered data is written to disk.
// If sync is true, forces an fsync to ensure data is persisted to disk.
// Returns error if any write, flush or sync operations fail.
func (s *Segment) Flush(sync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer : %w", err)
	}

	if sync || s.opts.SyncOnFlush {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync : %w", err)
		}
	}

	return nil
}

// Finalize marks the segment as closed by writing a final metadata entry
// and ensures all buffered data is written to the underlying storage.
// The method is idempotent and should be called before closing or archiving
// the segment to ensure data durability.
//
// Returns an error if either writing the final entry or flushing fails.
func (s *Segment) Finalize() error {
	payload := []byte("Final Entry")
	entry := domain.Entry{
		Payload: payload,
		Header: &domain.EntryHeader{
			Timestamp:   time.Now().UnixNano(),
			Type:        domain.EntrySegmentFinalize,
			PayloadSize: uint32(binary.Size(payload)),
		},
	}

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.BigEndian, entry); err != nil {
		return fmt.Errorf("failed to write final entry : %w", err)
	}

	entry.Header.Checksum = s.checksum.Calculate(buffer.Bytes())

	if err := s.Write(s.ctx, &entry); err != nil {
		return fmt.Errorf("failed to write final entry : %w", err)
	}

	if err := s.Flush(true); err != nil {
		return fmt.Errorf("failed to flush final entry : %w", err)
	}

	return nil
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
		return fmt.Errorf("failed to flush buffer : %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync : %w", err)
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
	payload := []byte(fmt.Sprintf("segment-%d", s.id))

	header := domain.Entry{
		Payload: payload,
		Header: &domain.EntryHeader{
			Sequence:    s.nextLSN,
			Timestamp:   time.Now().UnixNano(),
			Type:        domain.EntrySegmentHeader,
			PayloadSize: uint32(binary.Size(payload)),
		},
	}

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.BigEndian, header); err != nil {
		return fmt.Errorf("failed to write header entry : %w", err)
	}

	header.Header.Checksum = s.checksum.Calculate(buffer.Bytes())
	return s.Write(s.ctx, &header)
}

// Creates a segment filename by combining the configured prefix
// with the segment id. For example: "segment-0.log", "segment-1.log", etc.
func (s *Segment) generateName() string {
	return fmt.Sprintf("%s%d.log", s.opts.SegmentOptions.SegmentPrefix, s.id)
}
