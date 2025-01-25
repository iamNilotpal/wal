// Package compression provides data compression functionality using the zstd algorithm.
// It offers a thread-safe implementation with configurable compression levels
// and automatic optimization for small data sizes.
package compression

import (
	"fmt"
	"sync"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/klauspost/compress/zstd"
)

type Options struct {
	Level              uint8
	EncoderConcurrency uint8
	DecoderConcurrency uint8
}

// ZstdCompression implements CompressionPort using the zstd compression algorithm.
// It provides thread-safe compression and decompression operations with configurable
// compression levels. The implementation automatically handles small data sizes and
// cases where compression would not provide benefits.
type ZstdCompression struct {
	level   uint8         // Current compression level (1-9)
	mu      sync.RWMutex  // Protects concurrent access to compression state
	decoder *zstd.Decoder // Thread-safe decoder instance for decompression
	encoder *zstd.Encoder // Thread-safe encoder instance for compression
}

// Compression level constants define the trade-off between compression ratio and speed.
// Higher levels provide better compression at the cost of increased CPU usage and time.
const (
	FastestLevel uint8 = 1 // Optimized for speed with minimal compression
	DefaultLevel uint8 = 3 // Balanced between speed and compression ratio
	BestLevel    uint8 = 4 // Maximum compression ratio, higher CPU usage
)

// NewZstdCompression creates a new zstd compression instance with the specified level.
// It initializes both encoder and decoder with parallel processing capabilities.
// The compression level must be between FastestLevel (1) and BestLevel (9).
//
// Returns an error if:
// - The compression level is invalid
// - The encoder or decoder initialization fails
func NewZstdCompression(opts Options) (*ZstdCompression, error) {
	if err := Validate(
		&domain.CompressionOptions{
			Level:              opts.Level,
			EncoderConcurrency: opts.EncoderConcurrency,
			DecoderConcurrency: opts.DecoderConcurrency,
		},
	); err != nil {
		return nil, err
	}

	encoder, err := zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.EncoderLevel(opts.Level)),
		zstd.WithEncoderConcurrency(int(opts.EncoderConcurrency)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(int(opts.DecoderConcurrency)))
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}

	return &ZstdCompression{encoder: encoder, decoder: decoder, level: opts.Level}, nil
}

// Compress compresses the input data using zstd compression.
// It includes optimizations to:
// - Skip compression for small data blocks (< 64 bytes)
// - Return original data if compression doesn't reduce size
//
// The operation is thread-safe and can be called concurrently.
// Returns the compressed data or the original data if compression
// would not be beneficial.
func (z *ZstdCompression) Compress(data []byte) ([]byte, error) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	if len(data) < 64 {
		return data, nil
	}

	compressed := z.encoder.EncodeAll(data, nil)
	if len(compressed) < len(data) {
		return compressed, nil
	}

	return data, nil
}

// Decompress restores the original data from its compressed form.
// The operation is thread-safe and can be called concurrently.
//
// Returns an error if:
// - The input data is not valid zstd compressed data
// - Decompression fails for any other reason
func (z *ZstdCompression) Decompress(data []byte) ([]byte, error) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	decompressed, err := z.decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}

	return decompressed, nil
}

// Level returns the current compression level.
// The operation is thread-safe and can be called concurrently.
func (z *ZstdCompression) Level() uint8 {
	z.mu.RLock()
	defer z.mu.RUnlock()
	return z.level
}

// Close releases all resources used by the compression instance.
// This method should be called when the compression instance is no longer needed.
// After closing, the instance cannot be used for compression or decompression.
//
// The operation is thread-safe and ensures proper cleanup of both
// encoder and decoder resources.
func (z *ZstdCompression) Close() error {
	z.mu.Lock()
	defer z.mu.Unlock()

	if err := z.encoder.Close(); err != nil {
		return fmt.Errorf("error closing encoder : %w", err)
	}

	z.decoder.Close()
	return nil
}
