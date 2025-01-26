package config

import (
	"fmt"
)

const (
	// MinPayloadSize represents the absolute minimum size for a log entry.
	// While smaller writes are allowed, they will be optimized to this size
	// when auto-adjustment is enabled. This helps maintain I/O efficiency.
	MinPayloadSize = 128 // 128 bytes minimum.

	// SmallPayloadSize is optimized for frequent, latency-sensitive operations.
	// This size aligns with common page sizes in modern file systems and
	// provides good performance for small, frequent updates.
	SmallPayloadSize = 4096 // 4KB (typical page size).

	// MediumPayloadSize is designed for regular operational logging.
	// This size provides a good balance between write efficiency and
	// memory usage for typical application logging patterns.
	MediumPayloadSize = 32 * 1024 // 32KB

	// LargePayloadSize is intended for bulk operations and data dumps.
	// This size is optimized for high-throughput scenarios where
	// latency is less critical than overall throughput.
	LargePayloadSize = 256 * 1024 // 256KB.

	// MaxPayloadSize defines the absolute maximum size for a single log entry.
	// This limit helps prevent memory exhaustion and ensures consistent
	// performance characteristics across the system.
	MaxPayloadSize = 4 * 1024 * 1024 // 4MB.

	// Batch processing thresholds define how multiple log entries are grouped
	// together for improved I/O efficiency.

	// MinBatchSize represents the smallest batch size that provides
	// meaningful performance benefits over individual writes.
	MinBatchSize = 32 * 1024 // 32KB.

	// DefaultBatchSize is the recommended batch size for most operations.
	// This size provides optimal I/O performance while maintaining
	// reasonable memory usage and latency characteristics.
	DefaultBatchSize = 512 * 1024 // 512KB.

	// MaxBatchSize defines the upper limit for batch operations.
	// This prevents excessive memory usage and ensures predictable
	// latency even during high-load situations.
	MaxBatchSize = 8 * 1024 * 1024 // 8MB.

	// CompressionThreshold determines when to apply compression.
	// Entries larger than this size are candidates for compression,
	// balancing CPU overhead against storage and bandwidth savings.
	CompressionThreshold = 4 * 1024 // 64KB.

	// Version numbers for backwards compatibility control.
	// Version checking ensures that:
	// - Old data can be read correctly
	// - New features are properly handled
	// - Incompatible changes are detected early
	MinVersion = 1 // Oldest supported version.
	MaxVersion = 1 // Current version.
)

// PayloadConfig provides thread-safe configuration for WAL operations.
// It maintains size constraints and processing preferences while
// ensuring safe concurrent access to all settings.
type PayloadConfig struct {
	// MinSize enforces the minimum acceptable entry size
	MinSize uint32

	// MaxSize enforces the maximum acceptable entry size
	MaxSize uint32
}

// PayloadConfigOption defines the signature for configuration options.
// This pattern provides a flexible and type-safe way to modify
// configuration settings during initialization.
type PayloadConfigOption func(*PayloadConfig)

// WithMinSize sets the minimum entry size for the configuration.
// The provided size must be at least MinPayloadSize to be accepted.
// This option helps prevent excessive fragmentation from small entries.
func WithMinSize(size uint32) PayloadConfigOption {
	return func(c *PayloadConfig) {
		if size >= MinPayloadSize {
			c.MinSize = size
		}
	}
}

// WithMaxSize sets the maximum entry size for the configuration.
// The size must not exceed MaxPayloadSize to prevent resource exhaustion
// and maintain consistent performance characteristics.
func WithMaxSize(size uint32) PayloadConfigOption {
	return func(c *PayloadConfig) {
		if size <= MaxPayloadSize {
			c.MaxSize = size
		}
	}
}

// It initializes a LogConfig with default values and applies any
// provided configuration options. The resulting configuration is
// ready for immediate use in WAL operations.
func NewPayloadConfig(opts ...PayloadConfigOption) *PayloadConfig {
	cfg := DefaultPayloadConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// PayloadValidationError represents specific configuration validation errors.
type PayloadValidationError struct {
	Field   string
	Value   uint32
	Details string
}

func (e *PayloadValidationError) Error() string {
	return fmt.Sprintf("invalid configuration for %s (%d): %s", e.Field, e.Value, e.Details)
}

// Validate performs comprehensive validation of PayloadConfig settings
// It checks all configuration parameters against defined constraints
// and returns detailed errors for any validation failures
func (c *PayloadConfig) Validate() error {
	// Validate minimum size constraints.
	if c.MinSize < MinPayloadSize {
		return &PayloadValidationError{
			Field:   "MinSize",
			Value:   c.MinSize,
			Details: fmt.Sprintf("below minimum allowed value of %d", MinPayloadSize),
		}
	}

	if c.MinSize > MediumPayloadSize {
		return &PayloadValidationError{
			Field:   "MinSize",
			Value:   c.MinSize,
			Details: fmt.Sprintf("exceeds reasonable minimum threshold of %d", MediumPayloadSize),
		}
	}

	// Validate maximum size constraints.
	if c.MaxSize > MaxPayloadSize {
		return &PayloadValidationError{
			Field:   "MaxSize",
			Value:   c.MaxSize,
			Details: fmt.Sprintf("exceeds maximum allowed value of %d", MaxPayloadSize),
		}
	}

	if c.MaxSize < SmallPayloadSize {
		return &PayloadValidationError{
			Field:   "MaxSize",
			Value:   c.MaxSize,
			Details: fmt.Sprintf("below minimum efficient size of %d", SmallPayloadSize),
		}
	}

	// Validate size relationship constraints.
	if c.MinSize > c.MaxSize {
		return &PayloadValidationError{
			Field:   "MinSize",
			Value:   c.MinSize,
			Details: fmt.Sprintf("greater than MaxSize (%d)", c.MaxSize),
		}
	}

	return nil
}
