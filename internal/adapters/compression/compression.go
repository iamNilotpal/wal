package compression

import (
	"fmt"
	"runtime"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/pkg/errors"
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
func Validate(opts *domain.CompressionOptions) error {
	if opts == nil {
		return nil
	}

	// Validate compression level (1-9)
	if opts.Level < FastestLevel || opts.Level > BestLevel {
		return errors.NewValidationError(
			"level",
			opts.Level,
			fmt.Errorf("compression level must be between %d and %d, got %d", FastestLevel, BestLevel, opts.Level),
		)
	}

	// Validate encoder concurrency
	if opts.EncoderConcurrency > uint8(runtime.NumCPU()) {
		return errors.NewValidationError(
			"encoderConcurrency",
			opts.EncoderConcurrency,
			fmt.Errorf("encoder concurrency must be between 0 and %d, got %d", runtime.NumCPU(), opts.EncoderConcurrency),
		)
	}

	// Validate decoder concurrency
	if opts.DecoderConcurrency > uint8(runtime.NumCPU()) {
		return errors.NewValidationError(
			"decoderConcurrency",
			opts.DecoderConcurrency,
			fmt.Errorf("decoder concurrency must be between 0 and %d, got %d", runtime.NumCPU(), opts.DecoderConcurrency),
		)
	}

	return nil
}
