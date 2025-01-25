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
	"github.com/iamNilotpal/wal/pkg/pool"
)

var (
	// ErrSegmentClosed indicates operation on closed segment
	ErrSegmentClosed = errors.New("segment is closed")
)

// Segment represents a single log file on disk that stores sequential data.
// It provides thread-safe operations for writing log entries and managing segment state.
type Segment struct {
	// Configuration options for the WAL system, including segment behavior,
	// compression settings, and checksum parameters.
	options *domain.WALOptions

	// Interfaces for data integrity and compression operations.
	fs         ports.FileSystemPort  // Handles File System operations.
	checksum   ports.ChecksumPort    // Handles data integrity verification.
	compressor ports.CompressionPort // Handles data compression/decompression.

	// Core segment properties
	id         uint64           // Unique monotonically increasing identifier for the segment.
	path       string           // Absolute file path where segment data is stored.
	file       *os.File         // Operating system file handle for I/O operations.
	writer     *bufio.Writer    // Buffered writer to optimize write performance.
	bufferPool *pool.BufferPool // Buffer pool for better memory management.

	// Position tracking for data management and recovery.
	size            uint32    // Total segment size.
	nextLogSequence uint64    // Log Structured Number.
	createdAt       time.Time // Segment creation time.
	currOffset      uint64    // Current position where next write will occur.
	totalEntries    uint64    // TotalEntries tracks cumulative entries including deleted ones.
	prevOffset      uint64    // Last successfully written data offset, used for recovery and validation.

	// Rotation handling
	onRotate func(segment *Segment) // Callback function invoked when segment rotation occurs.

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

	// Last successfully written data offset, used for recovery and validation.
	PrevOffset uint64

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

// Record represents a single unit of data in the segment with its associated type.
// It encapsulates both the raw data (Payload) and metadata about the data (Type).
type Record struct {
	// Payload contains the raw binary data to be written.
	Payload []byte

	// Type indicates the semantic meaning and purpose of this record.
	// This determines how the Payload should be interpreted and processed.
	Type domain.EntryType
}

// Config holds the configuration parameters for creating a new segment.
type Config struct {
	// SegmentId is the unique identifier for this segment.
	// Each segment in a WAL must have a unique, monotonically increasing ID.
	SegmentId uint64

	// NextLogSequence represents the next available sequence number for log entries.
	NextLogSequence uint64

	// TotalSizeInBytes represents the current total size of entries in the segment.
	TotalSizeInBytes uint64

	// Options contains WAL-specific configuration parameters.
	Options *domain.WALOptions
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
func NewSegment(ctx context.Context, config *Config) (*Segment, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	fs := fs.NewLocalFileSystem()
	segment := Segment{
		fs:              fs,
		createdAt:       time.Now(),
		options:         config.Options,
		id:              config.SegmentId,
		nextLogSequence: config.NextLogSequence,
		totalEntries:    config.TotalSizeInBytes,
	}

	// Generate segment file path
	fileName := segment.generateName()
	path := filepath.Join(segment.options.Directory, segment.options.SegmentOptions.SegmentDirectory, fileName)

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
	if size >= segment.options.SegmentOptions.MaxSegmentSize {
		if err := file.Close(); err != nil {
			cancel()
			return nil, fmt.Errorf("error closing file : %w", err)
		}

		size = 0
		segment.id++
		segment.nextLogSequence = 0
		segment.totalEntries = 0

		fileName = segment.generateName()
		path = filepath.Join(segment.options.Directory, segment.options.SegmentOptions.SegmentDirectory, fileName)

		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error creating segment file : %w", err)
		}
	} else {
		segment.size = size
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
	segment.bufferPool = pool.NewBufferPool(int(segment.options.BufferSize))
	segment.writer = bufio.NewWriterSize(file, int(segment.options.BufferSize))

	if segment.options.ChecksumOptions.Enable {
		if segment.options.ChecksumOptions.Custom != nil {
			segment.checksum = segment.options.ChecksumOptions.Custom
		} else {
			segment.checksum = checksum.NewCheckSummer(segment.options.ChecksumOptions.Algorithm)
		}
	}

	if segment.options.CompressionOptions.Enable {
		segment.compressor, err = compression.NewZstdCompression(
			compression.Options{
				Level:              segment.options.CompressionOptions.Level,
				EncoderConcurrency: segment.options.CompressionOptions.EncoderConcurrency,
				DecoderConcurrency: segment.options.CompressionOptions.DecoderConcurrency,
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
	return s.nextLogSequence
}

// Returns metadata about this segment including file information and segment-specific details.
// It combines both filesystem metadata and segment-specific tracking information.
func (s *Segment) Info() (*SegmentInfo, error) {
	if s.closed.Load() {
		return nil, ErrSegmentClosed
	}

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
		Size:           stat.Size(),
		CreatedAt:      s.createdAt,
		CurrentOffset:  s.currOffset,
		PrevOffset:     s.prevOffset,
		Entries:        s.totalEntries,
		NextSequenceId: s.nextLogSequence,
	}, nil
}

// Write appends a new entry to the segment with proper synchronization, checksum,
// and compression. It enforces data integrity through checksums and supports optional
// disk syncing based on configuration. Forces immediate disk sync when true,
// overrides SyncOnWrite config.
func (s *Segment) Write(context context.Context, payload *Record, sync bool) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Initialize new entry with sequence number and metadata from payload.
	entry := domain.Entry{
		Header: &domain.EntryHeader{Sequence: s.nextLogSequence, Version: 0},
		Payload: &domain.EntryPayload{
			Payload: payload.Payload,
			Metadata: &domain.PayloadMetadata{
				PrevOffset: s.prevOffset,
				Type:       payload.Type,
				Timestamp:  time.Now().UnixNano(),
			},
		},
	}

	// First Proto marshalling before checksum calculation.
	encoded, err := entry.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to write entry : %w", err)
	}

	// Checksum is calculated only for the initial entry structure before any encoding or compression.
	// This includes:
	// - Header with sequence number and version.
	// - Raw payload data.
	// - Metadata (PrevOffset, Type, Timestamp).
	if s.options.ChecksumOptions.Enable {
		var buffer bytes.Buffer
		if err := binary.Write(&buffer, binary.LittleEndian, entry); err != nil {
			return fmt.Errorf("failed to write buffer entry : %w", err)
		}

		entry.Payload.Metadata.Checksum = s.checksum.Calculate(buffer.Bytes())

		// Re-marshal after checksum update to include it in the final encoding.
		encoded, err = entry.MarshalProto()
		if err != nil {
			return fmt.Errorf("failed to write entry : %w", err)
		}
	}

	// Apply compression if enabled.
	if s.options.CompressionOptions.Enable {
		compressed, err := s.compressor.Compress(encoded)
		if err != nil {
			return fmt.Errorf("failed to compress entry : %w", err)
		}

		encoded = compressed
	}

	// Set final payload size after all transformations (compression, encoding).
	entry.Header.PayloadSize = uint32(binary.Size(encoded))
	entrySize := binary.Size(encoded) + binary.Size(entry.Header)

	// Check if writing this entry would exceed segment size limit.
	if entrySize > int(s.options.SegmentOptions.MaxSegmentSize) {
		s, err = s.Rotate(context) // Create new segment if size limit reached.
		if err != nil {
			return fmt.Errorf("error rotating segment : %w", err)
		}
	}

	// Write header separately from payload for better format versioning support.
	if err := binary.Write(s.writer, binary.LittleEndian, entry.Header); err != nil {
		return fmt.Errorf("failed to write entry header : %w", err)
	}

	// Ensure complete write of encoded payload.
	// Verify complete payload write to protect against partial writes.
	if nn, err := s.writer.Write(encoded); err != nil {
		return fmt.Errorf("failed to write entry : %w", err)
	} else if nn != len(encoded) {
		return fmt.Errorf("short write: %d != %d", nn, len(encoded))
	}

	// Update segment metadata after successful write.
	s.totalEntries++
	s.nextLogSequence++
	s.prevOffset = s.currOffset
	s.currOffset += uint64(entrySize)

	// Sync to disk if explicitly requested or globally configured.
	if sync || s.options.SyncOnWrite {
		if err := s.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer : %w", err)
		}

		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file : %w", err)
		}
	}

	return nil
}

// Rotate creates a new segment and closes the current one,
// managing the transition between log segments.
//
// It ensures data consistency by following these steps:
//   - 1. Locks the current segment to prevent concurrent access.
//   - 2. Writes a special rotation record to mark the transition.
//   - 3. Safely closes the current segment.
//   - 4. Creates and returns a new segment.
func (s *Segment) Rotate(context context.Context) (*Segment, error) {
	if s.closed.Load() {
		return nil, ErrSegmentClosed
	}

	// Create a rotation record with the current segment's ID
	// This serves as a marker in the log to indicate where rotation occurred.
	buffer := s.bufferPool.Get()
	defer s.bufferPool.Put(buffer)

	buffer.WriteString(fmt.Sprintf("rotate-%d", s.id))
	entry := Record{Payload: buffer.Bytes(), Type: domain.EntryRotation}

	// Write the rotation marker to the current segment
	// We don't force a sync here since we'll be closing the segment immediately.
	// The Close operation will handle the final sync.
	if err := s.Write(context, &entry, false); err != nil {
		return nil, fmt.Errorf("failed to write rotation entry : %w", err)
	}

	// Close the current segment, which includes flushing and syncing all data.
	// This ensures all data is safely persisted before we transition to the new segment.
	if err := s.Close(context); err != nil {
		return nil, err
	}

	// Create a new segment with an incremented id.
	// The new segment starts fresh with:
	// - Reset sequence numbers (starting from 0).
	// - Reset size counter.
	// - Inheriting options from the current segment.
	segment, err := NewSegment(
		context,
		&Config{SegmentId: s.id + 1, NextLogSequence: 0, TotalSizeInBytes: 0, Options: s.options},
	)
	if err != nil {
		return nil, err
	}

	return segment, nil
}

// Flush ensures that all buffered data is written to disk.
// If sync is true, forces an fsync to ensure data is persisted to disk.
// Returns error if any write, flush or sync operations fail.
func (s *Segment) Flush(sync bool) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer : %w", err)
	}

	if sync || s.options.SyncOnFlush {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file : %w", err)
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
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	payload := []byte("final entry")
	entry := Record{Payload: payload, Type: domain.EntrySegmentFinalize}

	if err := s.Write(s.ctx, &entry, true); err != nil {
		return fmt.Errorf("failed to write final entry : %w", err)
	}

	return nil
}

// Close safely shuts down the segment and releases all associated resources.
// The method is thread-safe and ensures proper cleanup even under concurrent access.
// Once closed, any subsequent operations on the segment will return ErrSegmentClosed.
//
// Returns an error if any cleanup operation fails. The first error encountered
// during cleanup will be returned, and subsequent cleanup steps will be skipped.
func (s *Segment) Close(context context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrSegmentClosed
	}

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

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

// Transforms a raw Record into a structured Entry ready for storage.
//
// The preparation follows a specific sequence:
//   - Constructs a new Entry with current sequence and metadata
//   - Serializes the Entry to bytes using protocol buffers
//   - If enabled, calculates and sets checksum for data integrity
//   - If enabled, compresses the encoded data to reduce storage size
//   - Sets the final payload size in the entry header
//
// Returns:
//   - *domain.Entry: The prepared Entry structure with all metadata
//   - []byte: The final encoded bytes ready for storage
//   - error: Any error encountered during preparation
func (s *Segment) prepareEntry(record *Record) (*domain.Entry, []byte, error) {
	// Create a new Entry structure with metadata.
	entry := &domain.Entry{
		Header: &domain.EntryHeader{Version: 0, Sequence: s.nextLogSequence},
		Payload: &domain.EntryPayload{
			Payload: record.Payload,
			Metadata: &domain.PayloadMetadata{
				Type:       record.Type,
				PrevOffset: s.currOffset,
				Timestamp:  time.Now().UnixNano(),
			},
		},
	}

	// First serialization to get the base encoded form.
	encoded, err := entry.MarshalProto()
	if err != nil {
		return nil, nil, err
	}

	// Optional checksum calculation for data integrity.
	if s.options.ChecksumOptions.Enable {
		s.setChecksum(entry, encoded)

		// Re-encode after setting checksum to include it in the final bytes.
		encoded, err = entry.MarshalProto()
		if err != nil {
			return nil, nil, err
		}
	}

	// Optional compression to reduce storage size.
	if s.options.CompressionOptions.Enable {
		encoded, err = s.compressEntry(encoded)
		if err != nil {
			return nil, nil, err
		}
	}

	// Set the final size after all transformations.
	entry.Header.PayloadSize = uint32(len(encoded))
	return entry, encoded, nil
}

// Calculates and sets the checksum for an entry's data.
// This method updates the entry's metadata with a checksum value
// calculated from the provided data bytes. The checksum provides
// data integrity verification for stored entries.
func (s *Segment) setChecksum(entry *domain.Entry, data []byte) {
	checksum := s.checksum.Calculate(data)
	entry.Payload.Metadata.Checksum = checksum
}

// Compresses the provided data bytes using the segment's
// configured compression algorithm. This method is used to reduce the
// storage size of entries when compression is enabled.
//
// Returns:
//   - []byte: The compressed data.
//   - error: Any error encountered during compression.
func (s *Segment) compressEntry(data []byte) ([]byte, error) {
	compressed, err := s.compressor.Compress(data)
	if err != nil {
		return nil, err
	}
	return compressed, err
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
	entry := Record{Payload: payload, Type: domain.EntrySegmentHeader}
	return s.Write(s.ctx, &entry, true)
}

// Creates a segment filename by combining the configured prefix
// with the segment id. For example: "segment-0.log", "segment-1.log", etc.
func (s *Segment) generateName() string {
	return fmt.Sprintf("%s%d.log", s.options.SegmentOptions.SegmentPrefix, s.id)
}
