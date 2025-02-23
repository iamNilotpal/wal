package config

import (
	"fmt"

	"github.com/iamNilotpal/wal/pkg/errors"
)

const (
	// MinPayloadSize represents the absolute minimum size for a log entry.
	// While smaller writes are allowed, they will be optimized to this size
	// when auto-adjustment is enabled. This helps maintain I/O efficiency.
	MinPayloadSize = 80 // 80 bytes minimum. (header + payload = 128 bytes)

	// MaxPayloadSize defines the absolute maximum size for a single log entry.
	// This limit helps prevent memory exhaustion and ensures consistent
	// performance characteristics across the system.
	MaxPayloadSize = 8388608 - 13 // 7.99MB. (header + payload = 8MB)

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
	CompressionThreshold = 64 * 1024 // 64KB.

	// Version numbers for backwards compatibility control.
	// Version checking ensures that:
	// - Old data can be read correctly
	// - New features are properly handled
	// - Incompatible changes are detected early
	MinVersion = 1 // Oldest supported version.
	MaxVersion = 1 // Current version.
)

// PayloadOptions provides thread-safe configuration for WAL operations.
// It maintains size constraints and processing preferences while
// ensuring safe concurrent access to all settings.
type PayloadOptions struct {
	// MinSize enforces the minimum acceptable entry size
	MinSize uint32 `json:"minSize"`

	// MaxSize enforces the maximum acceptable entry size
	MaxSize uint32 `json:"maxSize"`
}

// PayloadConfigOption defines the signature for configuration options.
// This pattern provides a flexible and type-safe way to modify
// configuration settings during initialization.
type PayloadConfigOption func(*PayloadOptions)

// WithMinSize sets the minimum entry size for the configuration.
// The provided size must be at least MinPayloadSize to be accepted.
// This option helps prevent excessive fragmentation from small entries.
func WithMinSize(size uint32) PayloadConfigOption {
	return func(c *PayloadOptions) {
		if size >= MinPayloadSize {
			c.MinSize = size
		}
	}
}

// WithMaxSize sets the maximum entry size for the configuration.
// The size must not exceed MaxPayloadSize to prevent resource exhaustion
// and maintain consistent performance characteristics.
func WithMaxSize(size uint32) PayloadConfigOption {
	return func(c *PayloadOptions) {
		if size <= MaxPayloadSize {
			c.MaxSize = size
		}
	}
}

// It initializes a LogConfig with default values and applies any
// provided configuration options. The resulting configuration is
// ready for immediate use in WAL operations.
func NewPayloadConfig(opts ...PayloadConfigOption) *PayloadOptions {
	cfg := DefaultPayloadConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// Validate performs comprehensive validation of PayloadConfig settings
// It checks all configuration parameters against defined constraints
// and returns detailed errors for any validation failures
func (c *PayloadOptions) Validate() error {
	// Validate minimum size constraints.
	if c.MinSize < MinPayloadSize {
		return errors.NewValidationError(
			"minSize", c.MinSize, fmt.Errorf("below minimum allowed value of %d", MinPayloadSize),
		)
	}

	// Validate maximum size constraints.
	if c.MaxSize > MaxPayloadSize {
		return errors.NewValidationError(
			"maxSize", c.MaxSize, fmt.Errorf("exceeds maximum allowed value of %d", MaxPayloadSize),
		)
	}

	// Validate size relationship constraints.
	if c.MinSize > c.MaxSize {
		return errors.NewValidationError(
			"minSize", c.MinSize, fmt.Errorf("greater than MaxSize (%d)", c.MaxSize),
		)
	}

	return nil
}
