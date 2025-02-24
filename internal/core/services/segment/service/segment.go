package segment

import (
	"bufio"
	"context"
	"encoding/binary"
	stdErrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/adapters/fs"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/domain/config"
	"github.com/iamNilotpal/wal/internal/core/services/segment"
	"github.com/iamNilotpal/wal/pkg/errors"
	"github.com/iamNilotpal/wal/pkg/pool"
	"github.com/iamNilotpal/wal/pkg/system"
)

var (
	ErrSegmentClosed = stdErrors.New("operation failed: cannot access closed segment")
)

// NewSegment creates or opens a new log segment.
func NewSegment(ctx context.Context, config *Config) (*Segment, error) {
	segment := &Segment{}

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				if config == nil {
					return errors.NewValidationError("config", nil, fmt.Errorf("config is required"))
				}

				fs := fs.NewLocalFileSystem()

				segment.fs = fs
				segment.id = config.SegmentId
				segment.createdAt = time.Now()
				segment.options = config.Options
				segment.currOffset = config.LastOffset
				segment.totalEntries = config.TotalSizeInBytes
				segment.nextLogSequence = config.NextLogSequence

				// Generate segment file path
				fileName := segment.generateName()
				path := filepath.Join(segment.options.Directory, segment.options.SegmentOptions.Directory, fileName)

				// Create/Open segment file with read-write-append permissions.
				// 0644 permissions: owner can read/write, others can only read.
				file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				if err != nil {
					return fmt.Errorf("error creating segment file : %w", err)
				}

				stats, err := file.Stat()
				if err != nil {
					if err := file.Close(); err != nil {
						return fmt.Errorf("error closing file : %w", err)
					}
					return fmt.Errorf("error getting file stats : %w", err)
				}

				size := uint32(stats.Size())
				ctx, cancel := context.WithCancel(ctx)

				// If current segment exceeds max size, create new segment with incremented id.
				if size >= segment.options.SegmentOptions.MaxSegmentSize {
					if err := file.Close(); err != nil {
						cancel()
						return fmt.Errorf("error closing file : %w", err)
					}

					size = 0
					segment.id++
					segment.prevOffset = 0
					segment.currOffset = 0
					segment.totalEntries = 0
					segment.nextLogSequence = 0

					fileName = segment.generateName()
					path = filepath.Join(segment.options.Directory, segment.options.SegmentOptions.Directory, fileName)

					file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
					if err != nil {
						cancel()
						return fmt.Errorf("error creating segment file : %w", err)
					}
				} else if size > 0 {
					segment.prevOffset = uint64(stats.Size()) - config.LastOffset
				}

				// Move file pointer to end for appending.
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					cancel()
					if err := file.Close(); err != nil {
						return fmt.Errorf("error closing file : %w", err)
					}
					return err
				}

				segment.ctx = ctx
				segment.path = path
				segment.file = file
				segment.size = size
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
						if err := segment.Close(ctx); err != nil {
							return err
						}
						return fmt.Errorf("error creating compressor : %w", err)
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
						if err := segment.Close(ctx); err != nil {
							return err
						}
						return err
					}
				}

				return nil
			}
		}
	}); err != nil {
		return nil, err
	}

	return segment, nil
}

// Returns the unique identifier of the segment.
func (s *Segment) ID() (uint64, error) {
	if s.closed.Load() {
		return 0, ErrSegmentClosed
	}
	return s.id, nil
}

// Returns the next available log sequence number (LSN).
func (s *Segment) NextLogSequence() (uint64, error) {
	if s.closed.Load() {
		return 0, ErrSegmentClosed
	}
	return s.nextLogSequence, nil
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

// Persists a record to the segment with sophisticated handling of buffering,
// size limits, and durability guarantees. Forces immediate disk sync when true.
func (s *Segment) Write(ctx context.Context, record *Record, sync bool) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	// Set up a timeout context for the flush operation
	// This prevents potentially infinite blocking on I/O operations
	writeTimeoutCtx, cancel := context.WithTimeout(ctx, segment.WriteTimeout)
	defer cancel()

	return system.RunWithContext(writeTimeoutCtx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				// Prepare the entry for writing, which includes serialization and header generation.
				// This step converts the logical record into its physical storage format.
				entry, encoded, err := s.prepareEntry(record)
				if err != nil {
					return err
				}

				headerSize := binary.Size(entry.Header)
				entrySize := len(encoded) + headerSize

				// Certain entry types (metadata, rotation, checkpoint, finalize) are exempt
				// from size limits as they are essential for segment management.
				if record.Type == domain.EntryNormal {
					if err := s.checkSizeLimits(ctx, entrySize); err != nil {
						return err
					}
				}

				s.flushMu.Lock()
				defer s.flushMu.Unlock()

				// Check if we need to flush the buffer before this write.
				if s.shouldFlushBuffer(entrySize) {
					if err := s.flushLocked(sync); err != nil {
						return err
					}
				}

				// Write the entry header using binary encoding.
				// The header contains crucial metadata about the entry.
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

				// Update all segment metadata atomically after successful write.
				s.totalEntries++
				s.nextLogSequence++
				s.size += uint32(entrySize)
				s.prevOffset = s.currOffset
				s.currOffset += uint64(entrySize)

				// Perform final flush if sync is requested.
				// This ensures durability guarantees are met according to the caller's requirements.
				return s.flushLocked(sync)
			}
		}
	})
}

// ReadAt reads an entry from the segment file at the specified offset.
// It handles file seeking, reading, decompression (if enabled), and validation of the entry.
// The method is designed to be context-aware, allowing for cancellation or timeout via the provided context.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - offset: The byte offset in the segment file where the entry is located.
//
// Returns:
//   - *domain.Entry: The deserialized entry read from the file.
//   - error: An error if any step in the process fails, such as file seeking, reading, decompression, or validation.
func (s *Segment) ReadAt(ctx context.Context, offset int64) (*domain.Entry, error) {
	entry := &domain.Entry{}

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		if s.closed.Load() {
			return ErrSegmentClosed
		}

		s.mu.RLock()
		defer s.mu.RUnlock()

		// Create bounded reader to prevent large allocations.
		reader := io.NewSectionReader(s.file, offset, int64(s.options.PayloadOptions.MaxSize)+segment.HeaderSize)

		// Read the entry header from the file.
		var header domain.EntryHeader
		if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
			return fmt.Errorf("failed to read header : %w", err)
		}

		// Validate header before allocation.
		if err := header.Validate(); err != nil {
			return err
		}

		// Get a buffer from the buffer pool to hold the payload.
		buffer := s.bufferPool.Get()
		defer s.bufferPool.Put(buffer)

		payloadSize := int(header.PayloadSize)

		// Ensure the buffer has sufficient capacity.
		if buffer.Cap() < payloadSize {
			buffer.Grow(payloadSize)
		}

		entry.Header = &header
		payload := buffer.Bytes()[:payloadSize]

		// Read the payload from the file into the buffer.
		//
		// Method - 1
		// if nn, err := io.Copy(buffer, reader); err != nil && !stdErrors.Is(err, io.EOF) {
		// 	return fmt.Errorf("failed to read file : %w", err)
		// } else if nn < int64(totalSize) {
		// 	return fmt.Errorf("partial read, %d != %d", totalSize, nn)
		// }
		//
		// Method - 2
		if nn, err := io.ReadFull(reader, payload); err != nil && !stdErrors.Is(err, io.EOF) {
			return fmt.Errorf("failed to read file : %w", err)
		} else if nn < payloadSize {
			return fmt.Errorf("partial read, %d != %d", payloadSize, nn)
		}

		// If compression is enabled and the payload size exceeds the threshold, decompress the payload.
		if s.options.CompressionOptions.Enable && len(payload) > config.CompressionThreshold {
			decompressedPayload, err := s.compressor.Decompress(payload)
			if err != nil {
				return fmt.Errorf("failed decompress data : %w", err)
			}
			payload = decompressedPayload
		}

		return entry.UnMarshalProto(payload)
	}); err != nil {
		return nil, err
	}

	return entry, nil
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
func (s *Segment) Flush(ctx context.Context, sync bool) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	return system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				s.flushMu.Lock()
				defer s.flushMu.Unlock()
				return s.flushLocked(sync)
			}
		}
	})
}

// Finalize marks the segment as closed by writing a final metadata entry
// and ensures all buffered data is written to the underlying storage.
// The method is idempotent and should be called before closing or archiving
// the segment to ensure data durability.
//
// Returns an error if either writing the final entry or flushing fails.
func (s *Segment) Finalize(context context.Context) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	buffer := s.bufferPool.Get()
	defer s.bufferPool.Put(buffer)

	buffer.WriteString("final entry")
	entry := Record{Payload: buffer.Bytes(), Type: domain.EntrySegmentFinalize}

	if err := s.Write(context, &entry, true); err != nil {
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
func (s *Segment) Close(ctx context.Context) error {
	// First, atomically check and set the closed flag. This is a critical section
	// that ensures only one caller can proceed with closing operations.
	if !s.closed.CompareAndSwap(false, true) {
		return ErrSegmentClosed
	}

	return system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				// Acquire the flush mutex to ensure exclusive access during cleanup.
				// This prevents any concurrent flush operations from interfering
				// with the closing sequence.
				s.flushMu.Lock()
				defer s.flushMu.Unlock()

				// Cancel the segment's internal context to signal all background
				// operations and workers that they should terminate.
				s.cancel()
				// Wait for all background operations to complete before proceeding.
				// This ensures no operations are running when we close resources.
				s.wg.Wait()

				// Begin the resource cleanup sequence. Order is important here:
				// 1. Close compressor first to ensure all compressed data is flushed.
				if s.compressor != nil {
					if err := s.compressor.Close(); err != nil {
						return err
					}
				}

				// 2. Perform final flush to ensure any remaining data is written.
				// The 'true' parameter indicates this is the final flush during cleanup.
				if err := s.flushLocked(true); err != nil {
					return err
				}

				// 3. Finally, close the underlying file after all data operations.
				// are complete. Wrap the error to provide context about the failure.
				if s.file != nil {
					if err := s.file.Close(); err != nil {
						return fmt.Errorf("error closing file : %w", err)
					}
				}

				return nil
			}
		}
	})
}

// Sets up a callback function that will be invoked when
// the segment rotates due to size limits. This allows external components to
// be notified when a new segment is created and maintain their references.
//
// The handler function receives the newly created segment as its parameter.
// Only one handler can be registered at a time, subsequent registrations
// will override the previous handler.
func (s *Segment) RegisterRotationHandler(fn func(*Segment)) {
	s.onRotate = fn
}

// Performs flush operation when the mutex is already held.
func (s *Segment) flushLocked(sync bool) error {
	// First, flush any buffered data to the underlying writer.
	// This moves data from the in-memory buffer to the OS buffer.
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer : %w", err)
	}

	// Sync file to disk if either:
	// 1. The sync parameter is true (forced sync).
	// 2. The segment is configured to sync on every flush.
	// This ensures data durability by writing OS buffers to disk.
	if sync || s.options.SyncOnFlush || s.options.SyncOnWrite {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file : %w", err)
		}
	}

	return nil
}

// Ensures that adding a new entry won't exceed the segment's configured
// maximum size. If the new entry would cause the segment to exceed its size limit,
// this method initiates the rotation process to create a new segment.
// Returns an error if the rotation process fails during size limit handling.
func (s *Segment) checkSizeLimits(context context.Context, entrySize int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if adding the new entry would exceed the maximum segment size
	// The size check uses int conversion to handle potential large values safely
	if int(s.size)+entrySize > int(s.options.SegmentOptions.MaxSegmentSize) {
		if err := s.handleRotation(context); err != nil {
			return fmt.Errorf("segment rotation failed: %w", err)
		}
	}

	return nil
}

// Manages the segment rotation process and ensures proper handling
// of rotation callbacks. This method coordinates the transition between segments,
// maintaining consistency in the logging system while preserving any rotation
// callbacks that need to be executed.
func (s *Segment) handleRotation(context context.Context) error {
	// Create a new segment through the rotation process.
	newSegment, err := s.Rotate(context)
	if err != nil {
		return fmt.Errorf("failed to rotate segment: %w", err)
	}

	// Transfer rotation callback handler to the new segment.
	// This requires careful locking to ensure thread safety.
	newSegment.mu.Lock()
	newSegment.onRotate = s.onRotate
	s = newSegment // Update the segment reference to point to the new segment
	newSegment.mu.Unlock()

	// Execute rotation callback if one is registered.
	// This allows external components to react to segment rotations.
	if s.onRotate != nil {
		s.onRotate(s)
	}

	return nil
}

// Determines whether the internal buffer should be flushed based on
// available space and configured thresholds.
// Returns true if the buffer should be flushed, false otherwise.
func (s *Segment) shouldFlushBuffer(additionalBytes int) bool {
	available := s.writer.Available()
	bufferSize := s.writer.Size()

	// Perform an immediate flush if we can't accommodate the next write
	// This is a critical check to prevent buffer overflow
	if available < additionalBytes {
		return true
	}

	// Implement preventive flushing based on a minimum available space threshold
	// This helps maintain consistent write performance by avoiding situations
	// where the buffer becomes too full
	minAvailable := (bufferSize * segment.MinBufferAvailablePercent) / 100
	return available < minAvailable
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
		Header: &domain.EntryHeader{
			Version:  config.MaxVersion,
			Sequence: s.nextLogSequence,
		},
		Payload: &domain.EntryPayload{
			Payload: record.Payload,
			Metadata: &domain.PayloadMetadata{
				Type:       record.Type,
				PrevOffset: s.prevOffset,
				Timestamp:  time.Now().UnixNano(),
			},
		},
	}

	// First serialization to get the base encoded form.
	encoded, err := entry.MarshalProto(false)
	if err != nil {
		return nil, nil, err
	}

	// Optional checksum calculation for data integrity.
	if s.options.ChecksumOptions.Enable {
		s.setChecksum(entry, encoded)

		// Re-encode after setting checksum to include it in the final bytes.
		encoded, err = entry.MarshalProto(false)
		if err != nil {
			return nil, nil, err
		}
	}

	// Optional compression to reduce storage size.
	if s.options.CompressionOptions.Enable && len(encoded) > config.CompressionThreshold {
		encoded, err = s.compressEntry(encoded)
		if err != nil {
			return nil, nil, err
		}
	}

	// Set the final size after all transformations.
	entry.Header.PayloadSize = uint32(len(encoded))

	if err := entry.Validate(); err != nil {
		return nil, nil, err
	}
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
//   - EntrySegmentHeader type to distinguish from data entries
//
// This header entry allows readers to:
//   - Verify they are reading a valid segment file.
//   - Track when the segment was created.
//   - Match the segment file to its logical ID.
//
// An error is returned if writing the header entry fails.
func (s *Segment) writeEntryHeader() error {
	buffer := s.bufferPool.Get()
	defer s.bufferPool.Put(buffer)

	buffer.WriteString(fmt.Sprintf("segment-%d", s.id))
	entry := Record{Payload: buffer.Bytes(), Type: domain.EntrySegmentHeader}
	return s.Write(s.ctx, &entry, true)
}

// Creates a segment filename by combining the configured prefix
// with the segment id. For example: "segment-0.log", "segment-1.log", etc.
func (s *Segment) generateName() string {
	return fmt.Sprintf("%s%d.log", s.options.SegmentOptions.Prefix, s.id)
}
