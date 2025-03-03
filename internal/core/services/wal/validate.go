package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/segment"
	"github.com/iamNilotpal/wal/pkg/errors"
)

func Validate(opts *domain.WALOptions) error {
	// Check if directory exists and is writable or not.
	if opts.Directory != "" {
		if info, err := os.Stat(opts.Directory); err != nil {
			return errors.NewValidationError("directory", opts.Directory, fmt.Errorf("invalid directory: %w", err))
		} else if !info.IsDir() {
			return errors.NewValidationError(
				"directory",
				opts.Directory,
				fmt.Errorf("specified path is not a directory: %s", opts.Directory),
			)
		}

		// Test write permission by attempting to create a temporary file
		tmpFile := filepath.Join(opts.Directory, ".wal_write_test")
		if f, err := os.Create(tmpFile); err != nil {
			return errors.NewValidationError(
				"directory",
				opts.Directory,
				fmt.Errorf("directory is not writable: %s : %w", opts.Directory, err),
			)
		} else {
			f.Close()
			os.Remove(tmpFile)
		}
	}

	if opts.CleanupInterval != 0 && opts.CleanupInterval < time.Second {
		return errors.NewValidationError(
			"cleanupInterval",
			opts.CleanupInterval,
			fmt.Errorf("cleanup interval must be greater than or equal to 1s, got %s", opts.CleanupInterval),
		)
	}

	if opts.CompactInterval != 0 && opts.CompactInterval < time.Second {
		return errors.NewValidationError(
			"compactInterval",
			opts.CompactInterval,
			fmt.Errorf("compact interval must be greater than or equal to 1s, got %s", opts.CleanupInterval),
		)
	}

	if opts.FlushInterval != 0 && opts.FlushInterval < time.Second {
		return errors.NewValidationError(
			"flushInterval",
			opts.FlushInterval,
			fmt.Errorf("flush interval must be greater than or equal to 1s, got %s", opts.FlushInterval),
		)
	}

	if opts.RetentionDays != 0 && (opts.RetentionDays < 1 || opts.RetentionDays > 365) {
		return errors.NewValidationError(
			"retentionDays",
			opts.RetentionDays,
			fmt.Errorf("retention days must be between 1 and 365, got %d", opts.RetentionDays),
		)
	}

	if opts.MaxSegmentsKept < opts.MinSegmentsKept {
		return errors.NewValidationError(
			"maxSegmentsKept",
			opts.MaxSegmentsKept,
			fmt.Errorf(
				"maxSegmentsKept (%d) must be greater than or equal to minSegmentsKept (%d)",
				opts.MaxSegmentsKept, opts.MinSegmentsKept,
			),
		)
	}

	if opts.WriteTimeout < 0 {
		return errors.NewValidationError(
			"writeTimeout",
			opts.MaxSegmentsKept,
			fmt.Errorf("writeTimeout must be positive, got %s", opts.WriteTimeout),
		)
	}

	if opts.WriteTimeout != 0 && opts.WriteTimeout > segment.MaxWriteTimeout {
		return errors.NewValidationError(
			"writeTimeout",
			opts.MaxSegmentsKept,
			fmt.Errorf("writeTimeout must be less than or equal to %s, got %s", segment.MaxWriteTimeout, opts.WriteTimeout),
		)
	}

	if opts.BufferSize != 0 {
		if err := validateBufferSize(opts.BufferSize); err != nil {
			return err
		}
	}

	if opts.ChecksumOptions != nil && opts.ChecksumOptions.Enable {
		if err := checksum.Validate(opts.ChecksumOptions); err != nil {
			return err
		}
	}

	if opts.CompressionOptions != nil && opts.CompressionOptions.Enable {
		if err := compression.Validate(opts.CompressionOptions); err != nil {
			return err
		}
	}

	if opts.SegmentOptions != nil {
		if err := segment.Validate(opts.SegmentOptions); err != nil {
			return err
		}
	}

	if opts.PayloadOptions != nil {
		if err := opts.PayloadOptions.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func validateBufferSize(size uint32) error {
	// Check minimum size.
	if size < DefaultBufferSize {
		return errors.NewValidationError(
			"bufferSize",
			size,
			fmt.Errorf("buffer size must be at least 1MB (1048576 bytes), got %d bytes", size),
		)
	}

	// Check maximum size.
	if size > MaxBufferSize {
		return errors.NewValidationError(
			"bufferSize",
			size,
			fmt.Errorf("buffer size must not exceed 16MB (16777216 bytes), got %d bytes", size),
		)
	}

	// Optionally: Check if size is a power of 2.
	// This can be helpful for memory alignment and performance.
	if size&(size-1) != 0 {
		return errors.NewValidationError(
			"bufferSize",
			size,
			fmt.Errorf("buffer size must be a power of 2, got %d bytes", size),
		)
	}

	return nil
}
