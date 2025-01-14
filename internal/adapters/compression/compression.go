package compression

import (
	"fmt"
	"runtime"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

// Returns CompressionOptions struct initialized with
// recommended default values that provide a good balance between compression ratio
// and performance for most use cases.
func DefaultOptions() *domain.CompressionOptions {
	return &domain.CompressionOptions{
		Enable:             true,
		Level:              DefaultLevel,
		EncoderConcurrency: uint8(runtime.NumCPU()),
		DecoderConcurrency: uint8(runtime.NumCPU()),
	}
}

// Checks if the compression options are valid and returns an error if any option
// is outside acceptable bounds. It ensures Level and concurrency settings are within
// their allowed ranges and handles default values appropriately.
func Validate(input *domain.CompressionOptions) error {
	// Validate compression level (1-9)
	if input.Level < FastestLevel || input.Level > BestLevel {
		return fmt.Errorf("compression level must be between %d and %d, got %d", FastestLevel, BestLevel, input.Level)
	}

	// Validate encoder concurrency
	if input.EncoderConcurrency > uint8(runtime.NumCPU()) {
		return fmt.Errorf(
			"encoder concurrency must be between 0 and %d, got %d", runtime.NumCPU(), input.EncoderConcurrency,
		)
	}

	// Validate decoder concurrency
	if input.DecoderConcurrency > uint8(runtime.NumCPU()) {
		return fmt.Errorf(
			"decoder concurrency must be between 0 and %d, got %d", runtime.NumCPU(), input.DecoderConcurrency,
		)
	}

	return nil
}
