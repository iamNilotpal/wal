package wal

import (
	"runtime"
	"strings"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/services/segment"
)

const (
	MinSegmentsKept = 2
	MaxSegmentsKept = 10
	Directory       = "./logs"

	MinBufferSize     = 4096     // 4KB
	DefaultBufferSize = 1048576  // 1MB
	MaxBufferSize     = 16777216 // 16MB

	DefaultFlushInterval   = time.Duration(time.Second * 5)  // 5s
	DefaultCompactInterval = time.Duration(time.Hour * 1)    // 1h
	DefaultCleanupInterval = time.Duration(time.Minute * 15) // 15m
)

func prepareDefaults(opts *domain.WALOptions) *domain.WALOptions {
	if opts.BufferSize == 0 || opts.BufferSize < MinBufferSize {
		opts.BufferSize = DefaultBufferSize
	}

	if opts.BufferSize > MaxBufferSize {
		opts.BufferSize = MaxBufferSize
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
		opts.MaxSegmentsKept = MaxSegmentsKept
	}

	if opts.MinSegmentsKept == 0 {
		opts.MinSegmentsKept = MinSegmentsKept
	}

	if opts.RetentionDays == 0 {
		opts.MinSegmentsKept = MinSegmentsKept
	}

	if strings.TrimSpace(opts.Directory) == "" {
		opts.Directory = Directory
	}

	if opts.SegmentOptions == nil {
		opts.SegmentOptions = segment.DefaultOptions()
	} else {
		if strings.TrimSpace(opts.SegmentOptions.SegmentDirectory) == "" {
			opts.SegmentOptions.SegmentDirectory = segment.SegmentDirectory
		}

		if strings.TrimSpace(opts.SegmentOptions.SegmentPrefix) == "" {
			opts.SegmentOptions.SegmentPrefix = segment.SegmentPrefix
		}
	}

	if opts.ChecksumOptions == nil {
		opts.ChecksumOptions = checksum.DefaultOptions()
	} else {
		opts.ChecksumOptions.Enable = true
		if strings.TrimSpace(string(opts.ChecksumOptions.Algorithm)) == "" {
			opts.ChecksumOptions.Algorithm = checksum.CRC32IEEE
		}
	}

	if opts.CompressionOptions == nil {
		opts.CompressionOptions = compression.DefaultOptions()
	} else {
		opts.CompressionOptions.Enable = true

		if opts.CompressionOptions.DecoderConcurrency == 0 {
			opts.CompressionOptions.DecoderConcurrency = uint8(runtime.NumCPU())
		}

		if opts.CompressionOptions.EncoderConcurrency == 0 {
			opts.CompressionOptions.EncoderConcurrency = uint8(runtime.NumCPU())
		}
	}

	return opts
}
