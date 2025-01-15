package wal

import (
	"strings"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/segment"
)

const (
	DefaultMinSegmentsKept = 2
	DefaultMaxSegmentsKept = 10
	DefaultDirectory       = "/logs"

	DefaultMinBufferSize = 4096     // 4KB
	DefaultMaxBufferSize = 16777216 // 16MB

	DefaultFlushInterval   = time.Duration(time.Second * 5)  // 5s
	DefaultCompactInterval = time.Duration(time.Hour * 1)    // 1h
	DefaultCleanupInterval = time.Duration(time.Minute * 15) // 15m
)

func prepareDefaults(opts *domain.WALOptions) *domain.WALOptions {
	if opts.BufferSize < DefaultMinBufferSize {
		opts.BufferSize = DefaultMinBufferSize
	}

	if opts.BufferSize > DefaultMaxBufferSize {
		opts.BufferSize = DefaultMaxBufferSize
	}

	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = DefaultCleanupInterval
	}

	if opts.CompactInterval == 0 {
		opts.CompactInterval = DefaultCompactInterval
	}

	if opts.FlushInterval == 0 {
		opts.FlushInterval = DefaultFlushInterval
	}

	if opts.MaxSegmentsKept == 0 {
		opts.MaxSegmentsKept = DefaultMaxSegmentsKept
	}

	if opts.MinSegmentsKept == 0 {
		opts.MinSegmentsKept = DefaultMinSegmentsKept
	}

	if opts.RetentionDays == 0 {
		opts.MinSegmentsKept = DefaultMinSegmentsKept
	}

	if strings.TrimSpace(opts.Directory) == "" {
		opts.Directory = DefaultDirectory
	}

	if opts.SegmentOptions == nil {
		opts.SegmentOptions = segment.DefaultOptions()
	} else {
		if strings.TrimSpace(opts.SegmentOptions.SegmentDirectory) == "" {
			opts.SegmentOptions.SegmentDirectory = segment.DefaultSegmentDirectory
		}

		if strings.TrimSpace(opts.SegmentOptions.SegmentPrefix) == "" {
			opts.SegmentOptions.SegmentPrefix = segment.DefaultSegmentPrefix
		}
	}

	if opts.ChecksumOptions == nil {
		opts.ChecksumOptions = checksum.DefaultOptions()
	}

	if opts.CompressionOptions == nil {
		opts.CompressionOptions = compression.DefaultOptions()
	}

	return opts
}
