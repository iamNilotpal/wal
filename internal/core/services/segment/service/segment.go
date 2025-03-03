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
	ErrInvalidChecksum = stdErrors.New("wal: invalid checksum")
	ErrSegmentClosed   = stdErrors.New("operation failed: cannot access closed segment")
)

// New creates or opens a new log segment.
func New(ctx context.Context, config *Config) (*Segment, error) {
	segment := &Segment{
		id:              config.SegmentId,
		options:         config.Options,
		createdAt:       time.Now(),
		currentOffset:   config.LastOffset,
		nextLogSequence: config.NextLogSequence,
		size:            uint32(config.TotalSizeInBytes),
	}

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				if config == nil {
					return errors.NewValidationError(
						"config", nil, segment.formatError("config is required", nil),
					)
				}

				fs := fs.NewLocalFileSystem()
				segment.fs = fs

				// Generate segment file path
				fileName := segment.generateName()
				path := filepath.Join(segment.options.Directory, segment.options.SegmentOptions.Directory, fileName)

				// Create/Open segment file with read-write-append permissions.
				// 0644 permissions: owner can read/write, others can only read.
				file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				if err != nil {
					return segment.formatError("error creating segment file", err)
				}

				stats, err := file.Stat()
				if err != nil {
					if err := file.Close(); err != nil {
						return segment.formatError("error closing file", err)
					}
					return segment.formatError("error getting file stats", err)
				}

				size := uint32(stats.Size())
				ctx, cancel := context.WithCancel(ctx)

				// If current segment exceeds max size, create new segment with incremented id.
				if size >= segment.options.SegmentOptions.MaxSegmentSize {
					if err := file.Close(); err != nil {
						cancel()
						return segment.formatError("error closing file", err)
					}

					size = 0
					segment.id++
					segment.totalEntries = 0
					segment.currentOffset = 0
					segment.previousOffset = 0
					segment.nextLogSequence = 0

					fileName = segment.generateName()
					path = filepath.Join(segment.options.Directory, segment.options.SegmentOptions.Directory, fileName)

					file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
					if err != nil {
						cancel()
						return segment.formatError("error creating segment file", err)
					}
				} else if size > 0 {
					segment.previousOffset = uint64(stats.Size()) - config.LastOffset
				}

				// Move file pointer to end for appending.
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					cancel()
					if err := file.Close(); err != nil {
						return segment.formatError("error closing file", err)
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
					if compressor, err := compression.NewZstdCompression(
						compression.Options{
							Level:              segment.options.CompressionOptions.Level,
							EncoderConcurrency: segment.options.CompressionOptions.EncoderConcurrency,
							DecoderConcurrency: segment.options.CompressionOptions.DecoderConcurrency,
						},
					); err != nil {
						if err := segment.Close(ctx); err != nil {
							return err
						}
						return segment.formatError("error creating compressor", err)
					} else {
						segment.compressor = compressor
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
		return nil, s.formatError("failed to load file stats", err)
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
		Entries:        s.totalEntries,
		CurrentOffset:  s.currentOffset,
		PreviousOffset: s.previousOffset,
		NextSequenceId: s.nextLogSequence,
	}, nil
}

// Write persists a record to the segment with sophisticated handling of buffering,
// size limits, and durability guarantees. If `sync` is true, it forces an immediate
// disk synchronization to ensure the write is durable.
func (s *Segment) Write(ctx context.Context, record *Record, sync bool) error {
	// Ensure the segment is open before proceeding.
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	// Set up a timeout context to prevent infinite blocking on I/O operations.
	writeTimeoutCtx, cancel := context.WithTimeout(ctx, s.options.WriteTimeout)
	defer cancel()

	return system.RunWithContext(writeTimeoutCtx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				// Estimate entry size for rotation check (more accurate size calculated after preparation).
				headerSize := binary.Size(domain.EntryHeader{})
				estimatedSize := len(record.Payload) + headerSize

				// Check if rotation is needed before proceeding with the write.
				// Only normal entries are subject to size limits; metadata entries must always be written.
				if record.Type == domain.EntryNormal {
					if newSegment, err := s.checkSizeLimits(ctx, estimatedSize); err != nil {
						return s.formatError("segment rotation failed", err)
					} else if newSegment != nil {
						// Redirect the write to the new segment.
						return newSegment.Write(ctx, record, sync)
					}
				}

				// Prepare the entry for writing, including serialization, setting headers,
				// applying compression, checksums, or other transformations if configured.
				entry, encoded, err := s.prepareEntry(record)
				if err != nil {
					return err
				}

				// Compute actual entry size after encoding.
				actualEntrySize := len(encoded) + headerSize

				// Check if the write buffer needs to be flushed to make space for this entry.
				if s.shouldFlushBuffer(actualEntrySize) {
					if err := s.flush(); err != nil {
						return err
					}
				}

				// Perform the actual write operation, ensuring durability if `sync` is enabled.
				return s.writeEntry(entry, encoded, sync)
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

		// Create bounded reader to prevent large allocations.
		reader := io.NewSectionReader(s.file, offset, int64(s.options.PayloadOptions.MaxSize)+segment.HeaderSize)

		// Read the entry header from the file.
		var header domain.EntryHeader
		if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
			return s.formatError("failed to read header", err)
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

		// Method - 1
		// Read the payload from the file into the buffer.
		if nn, err := io.Copy(buffer, reader); err != nil && !stdErrors.Is(err, io.EOF) {
			return s.formatError("failed to read file", err)
		} else if nn < int64(payloadSize) {
			return s.formatError(fmt.Sprintf("partial read, %d != %d", payloadSize, nn), err)
		}

		// Method - 2
		// Read the payload from the file into the buffer.
		// if nn, err := io.ReadFull(reader, payload); err != nil && !stdErrors.Is(err, io.EOF) {
		// 	return s.formatError("failed to read file", err)
		// } else if nn < payloadSize {
		// 	return s.formatError(fmt.Sprintf("partial read, %d != %d", payloadSize, nn), err)
		// }

		// If compression is enabled and the payload is marked as compressed, decompress it.
		if s.options.CompressionOptions.Enable && entry.Header.Compressed {
			decompressedPayload, err := s.compressor.Decompress(payload)
			if err != nil {
				return s.formatError("failed decompress data", err)
			}

			buffer.Reset()
			buffer.Grow(len(decompressedPayload))
			buffer.Write(decompressedPayload)
			payload = buffer.Bytes()[:len(decompressedPayload)]
		}

		if err := entry.UnMarshalProto(payload); err != nil {
			return err
		}

		if s.options.ChecksumOptions.VerifyOnRead {
			ok, err := s.verifyChecksum(entry)
			if err != nil {
				return s.formatError("failed to validate checksum", err)
			}

			if !ok {
				return s.formatError("invalid signature", ErrInvalidChecksum)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return entry, nil
}

// ReadAll retrieves all log entries from the segment file and returns them as a slice of domain.Entry.
// This method ensures safe concurrent access and efficient memory usage while supporting context-based cancellation.
//
// The function operates by sequentially reading the segment file, extracting entry headers and payloads, and
// deserializing them into domain.Entry objects. It leverages a buffer pool to optimize memory allocation and
// supports decompression when necessary. In case of errors, it returns all successfully read entries alongside the error.
func (s *Segment) ReadAll(ctx context.Context) ([]*domain.Entry, error) {
	var entries []*domain.Entry

	if s.closed.Load() {
		return entries, ErrSegmentClosed
	}

	if err := system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			{
				var offset int64
				var errToReturn error

			outerLoop:
				for offset < int64(s.size) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						{
							var header domain.EntryHeader
							// Create a section reader to read the next entry header and payload.
							sectionReader := io.NewSectionReader(
								s.file, offset, int64(s.options.PayloadOptions.MaxSize)+segment.HeaderSize,
							)

							// Read the entry header (metadata) from the segment file.
							if err := binary.Read(sectionReader, binary.LittleEndian, &header); err != nil {
								if stdErrors.Is(err, io.EOF) {
									break outerLoop
								}
								errToReturn = err
								break outerLoop
							}

							// Retrieve a buffer from the pool to minimize allocations.
							buffer := s.bufferPool.Get()
							payloadSize := int(header.PayloadSize)
							entry := &domain.Entry{Header: &header}

							// Ensure the buffer has sufficient capacity.
							if buffer.Cap() < payloadSize {
								buffer.Grow(payloadSize)
							}

							// Method - 1
							// Read the full payload into the buffer.
							if _, err := io.Copy(buffer, sectionReader); err != nil && !stdErrors.Is(err, io.EOF) {
								s.bufferPool.Put(buffer)
								errToReturn = s.formatError("failed to read payload", err)
								break outerLoop
							}

							// Method - 2
							// Read the full payload into the buffer.
							// if _, err := io.ReadFull(sectionReader, payload); err != nil && !stdErrors.Is(err, io.EOF) {
							// 	s.bufferPool.Put(buffer)
							// 	errToReturn = s.formatError("failed to read payload", err)
							// 	break outerLoop
							// }

							payload := buffer.Bytes()[:payloadSize]

							// If compression is enabled and the payload is marked as compressed, decompress it.
							if s.options.CompressionOptions.Enable && header.Compressed {
								decompressedPayload, err := s.compressor.Decompress(payload)
								if err != nil {
									s.bufferPool.Put(buffer)
									errToReturn = s.formatError("failed decompress data", err)
									break outerLoop
								}

								// Reset buffer and store decompressed data.
								buffer.Reset()
								buffer.Grow(len(decompressedPayload))
								buffer.Write(decompressedPayload)
								payload = buffer.Bytes()
							}

							// Deserialize the payload into the Entry object.
							if err := entry.UnMarshalProto(payload); err != nil {
								s.bufferPool.Put(buffer)
								errToReturn = err
								break outerLoop
							}

							if s.options.ChecksumOptions.VerifyOnRead {
								ok, err := s.verifyChecksum(entry)
								if err != nil {
									errToReturn = s.formatError("failed to validate checksum", err)
									break outerLoop
								}

								if !ok {
									errToReturn = s.formatError("invalid signature", ErrInvalidChecksum)
									break outerLoop
								}
							}

							s.bufferPool.Put(buffer)
							entries = append(entries, entry)
							// Move the offset forward by the size of the read entry.
							offset += int64(header.PayloadSize + segment.HeaderSize)
						}
					}
				}
				return errToReturn
			}
		}
	}); err != nil {
		return entries, err
	}

	return entries, nil
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

	buffer := s.bufferPool.Get()
	defer s.bufferPool.Put(buffer)

	// Create a rotation record with the current segment's ID.
	// This serves as a marker in the log to indicate where rotation occurred.
	buffer.WriteString(fmt.Sprintf("rotate-%d", s.id))
	rotationEntry := Record{Payload: buffer.Bytes(), Type: domain.EntryRotation}

	// Prepare the rotation entry with proper sequence numbers while holding the lock.
	// This maintains the atomic nature of WAL entries even during rotation.
	entry, encoded, err := s.prepareEntry(&rotationEntry)
	if err != nil {
		return nil, s.formatError("failed to prepare rotation entry", err)
	}

	// Write rotation entry, ensuring complete write.
	if err := s.writeEntry(entry, encoded, false); err != nil {
		return nil, err
	}

	// Close the current segment, which includes flushing and syncing all data
	// This ensures all data is safely persisted before we transition to the new segment
	if err := s.Close(context); err != nil {
		return nil, s.formatError("failed to close segment during rotation", err)
	}

	// Create a new segment with an incremented ID.
	// The new segment starts fresh with reset sequence numbers and size counter
	segment, err := New(
		context,
		&Config{
			NextLogSequence:  0,
			TotalSizeInBytes: 0,
			SegmentId:        s.id + 1,
			Options:          s.options,
		},
	)
	if err != nil {
		return nil, s.formatError("failed to create new segment during rotation", err)
	}

	return segment, nil
}

// Flush ensures that all buffered data is written to disk.
func (s *Segment) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return ErrSegmentClosed
	}

	return system.RunWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return s.flush()
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
		return s.formatError("failed to write final entry", err)
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
				if err := s.flush(); err != nil {
					return err
				}

				// 3. Finally, close the underlying file after all data operations.
				// are complete. Wrap the error to provide context about the failure.
				if s.file != nil {
					if err := s.file.Close(); err != nil {
						return s.formatError("error closing file", err)
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
func (s *Segment) flush() error {
	// First, flush any buffered data to the underlying writer.
	// This moves data from the in-memory buffer to the OS buffer.
	if err := s.writer.Flush(); err != nil {
		return s.formatError("failed to flush buffer", err)
	}

	// This ensures data durability by writing OS buffers to disk.
	if err := s.file.Sync(); err != nil {
		return s.formatError("failed to sync file", err)
	}

	return nil
}

// writeEntry performs the low-level operation of writing a prepared entry
// to the segment's underlying writer. It handles flushing the buffer if
// necessary, writes the entry header and payload, updates segment metadata,
// and optionally performs a disk sync.
//
// The method is designed to be called with the segment's main mutex (s.mu)
// held to ensure atomicity of operations that modify segment metadata.
// It uses a separate I/O mutex (ioMutex) to protect the
// actual I/O operations, ensuring only one write or flush operation
// occurs at a time. This prevents data races and ensures data integrity.
//
// Parameters:
//   - entry: The prepared entry to be written. This includes the header
//     and other metadata.
//   - encoded: The encoded payload of the entry, ready to be written to the
//     underlying writer.
//   - sync: A boolean indicating whether to force a disk sync (fsync) after
//     writing the entry.  This provides durability guarantees at the
//     expense of performance.
//
// Returns:
//   - error: An error if any step in the process fails, or nil on success.
func (s *Segment) writeEntry(entry *domain.Entry, encoded []byte, sync bool) error {
	// Calculate the size of the entry header.
	headerSize := binary.Size(entry.Header)
	// Calculate the actual size of the entire entry (header + payload).
	actualEntrySize := len(encoded) + headerSize

	// Check if the buffer needs to be flushed before writing this entry.
	// This ensures that there is enough space in the buffer for the new entry.
	if s.shouldFlushBuffer(actualEntrySize) {
		if err := s.flush(); err != nil {
			return s.formatError("failed to flush buffer", err)
		}
	}

	// Write the entry header to the underlying writer.
	if err := binary.Write(s.writer, binary.LittleEndian, entry.Header); err != nil {
		return s.formatError("failed to write entry header", err)
	}

	// Write the encoded payload to the underlying writer.
	bytesWritten, err := s.writer.Write(encoded)
	if err != nil {
		return s.formatError("failed to write entry payload", err)
	}
	// Verify that the entire payload was written.
	if bytesWritten != len(encoded) {
		return s.formatError(
			fmt.Sprintf("short write occurred: %d bytes written, expected %d", bytesWritten, len(encoded)), io.ErrShortWrite,
		)
	}

	// Update segment metadata.
	s.totalEntries++
	s.nextLogSequence++
	s.size += uint32(actualEntrySize)
	s.previousOffset = s.currentOffset
	s.currentOffset += uint64(actualEntrySize)

	// Flush the buffer to disk if sync is requested. This provides
	// durability guarantees at the expense of performance.
	if sync || s.options.SyncOnWrite {
		if err := s.flush(); err != nil {
			return s.formatError("failed to flush buffer", err)
		}
	}

	return nil
}

// Ensures that adding a new entry won't exceed the segment's configured
// maximum size. If the new entry would cause the segment to exceed its size limit,
// this method initiates the rotation process to create a new segment.
// Returns an error if the rotation process fails during size limit handling.
func (s *Segment) checkSizeLimits(context context.Context, entrySize int) (*Segment, error) {
	// Check if adding the new entry would exceed the maximum segment size
	// The size check uses int conversion to handle potential large values safely
	if int(s.size)+entrySize > int(s.options.SegmentOptions.MaxSegmentSize) {
		segment, err := s.handleRotation(context)
		if err != nil {
			return nil, err
		}

		if segment != nil {
			return segment, nil
		}
	}

	return nil, nil
}

// Manages the segment rotation process and ensures proper handling
// of rotation callbacks. This method coordinates the transition between segments,
// maintaining consistency in the logging system while preserving any rotation
// callbacks that need to be executed.
func (s *Segment) handleRotation(context context.Context) (*Segment, error) {
	// Create a new segment through the rotation process.
	newSegment, err := s.Rotate(context)
	if err != nil {
		return nil, s.formatError("failed to rotate segment", err)
	}

	newSegment.mu.Lock()
	defer newSegment.mu.Unlock()

	// Transfer rotation callback handler to the new segment.
	// Execute rotation callback if one is registered.
	newSegment.onRotate = s.onRotate
	if newSegment.onRotate != nil {
		newSegment.onRotate(newSegment)
	}

	return newSegment, nil
}

// Determines whether the internal buffer should be flushed based on
// available space and configured thresholds.
// Returns true if the buffer should be flushed, false otherwise.
func (s *Segment) shouldFlushBuffer(additionalBytes int) bool {
	available := s.writer.Available()
	bufferSize := s.options.BufferSize

	// Perform an immediate flush if we can't accommodate the next write
	// This is a critical check to prevent buffer overflow
	if available < additionalBytes {
		return true
	}

	// Preventive flushing based on a minimum available space threshold
	// This helps maintain consistent write performance by avoiding situations
	// where the buffer becomes too full.
	//
	// Calculate the minimum available space as a percentage of total buffer size
	// For example, if MinBufferAvailablePercent is 20, we want to maintain at least
	// 20% of the buffer as available space.
	minAvailable := int(float64(bufferSize) * segment.MinBufferAvailablePercent / 100.0)
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
				PrevOffset: s.previousOffset,
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
		entry.Header.Compressed = true
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

func (s *Segment) verifyChecksum(entry *domain.Entry) (bool, error) {
	newEntry := domain.Entry{
		Header: entry.Header,
		Payload: &domain.EntryPayload{
			Payload: entry.Payload.Payload,
			Metadata: &domain.PayloadMetadata{
				Type:       entry.Payload.Metadata.Type,
				Timestamp:  entry.Payload.Metadata.Timestamp,
				PrevOffset: entry.Payload.Metadata.PrevOffset,
			},
		},
	}

	encoded, err := newEntry.MarshalProto(false)
	if err != nil {
		return false, s.formatError("failed to marshal proto", err)
	}

	return s.checksum.Verify(encoded, entry.Payload.Metadata.Checksum), nil
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

// formatError creates a new error with added context, including the segment's ID
// and a descriptive message, wrapping the original error. This provides more
// informative error messages for debugging and troubleshooting.
func (s *Segment) formatError(message string, err error) error {
	return fmt.Errorf("segment %d: %s: %w", s.id, message, err)
}

// Creates a segment filename by combining the configured prefix
// with the segment id. For example: "segment-0.log", "segment-1.log", etc.
func (s *Segment) generateName() string {
	return fmt.Sprintf("%s%d.log", s.options.SegmentOptions.Prefix, s.id)
}
