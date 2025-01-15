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
)

func Validate(opts *domain.WALOptions) error {
	// Check if directory exists and is writable or not.
	if opts.Directory != "" {
		if info, err := os.Stat(opts.Directory); err != nil {
			return fmt.Errorf("invalid directory: %w", err)
		} else if !info.IsDir() {
			return fmt.Errorf("specified path is not a directory: %s", opts.Directory)
		}

		// Test write permission by attempting to create a temporary file
		tmpFile := filepath.Join(opts.Directory, ".wal_write_test")
		if f, err := os.Create(tmpFile); err != nil {
			return fmt.Errorf("directory is not writable: %s : %w", opts.Directory, err)
		} else {
			f.Close()
			os.Remove(tmpFile)
		}
	}

	if opts.CleanupInterval < time.Second {
		return fmt.Errorf("cleanup interval must be greater than or equal to 1s, got %s", opts.CleanupInterval)
	}

	if opts.CompactInterval < time.Second {
		return fmt.Errorf("compact interval must be greater than or equal to 1s, got %s", opts.CleanupInterval)
	}

	if opts.FlushInterval < time.Second {
		return fmt.Errorf("flush interval must be greater than or equal to 1s, got %s", opts.FlushInterval)
	}

	if opts.RetentionDays != 0 && (opts.RetentionDays < 1 || opts.RetentionDays > 365) {
		return fmt.Errorf("retention days must be between 1 and 365, got %d", opts.RetentionDays)
	}

	if opts.MaxSegmentsKept < opts.MinSegmentsKept {
		return fmt.Errorf(
			"maxSegmentsKept (%d) must be greater than or equal to minSegmentsKept (%d)",
			opts.MaxSegmentsKept, opts.MinSegmentsKept,
		)
	}

	if err := validateBufferSize(opts.BufferSize); err != nil {
		return err
	}

	if opts.ChecksumOptions.Enable {
		if err := checksum.Validate(opts.ChecksumOptions); err != nil {
			return err
		}
	}

	if opts.CompressionOptions.Enable {
		if err := compression.Validate(opts.CompressionOptions); err != nil {
			return err
		}
	}

	if err := segment.Validate(opts.SegmentOptions); err != nil {
		return err
	}

	return nil
}

func validateBufferSize(size uint32) error {
	// Check minimum size
	if size < DefaultMinBufferSize {
		return fmt.Errorf("buffer size must be at least 4KB (4096 bytes), got %d bytes", size)
	}

	// Check maximum size
	if size > DefaultMaxBufferSize {
		return fmt.Errorf("buffer size must not exceed 16MB (16777216 bytes), got %d bytes", size)
	}

	// Optionally: Check if size is a power of 2
	// This can be helpful for memory alignment and performance
	if size&(size-1) != 0 {
		return fmt.Errorf("buffer size must be a power of 2, got %d bytes", size)
	}

	return nil
}
