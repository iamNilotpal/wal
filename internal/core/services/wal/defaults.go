package wal

import (
	"runtime"
	"strings"
	"time"

	"github.com/iamNilotpal/wal/internal/adapters/checksum"
	"github.com/iamNilotpal/wal/internal/adapters/compression"
	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/domain/config"
	"github.com/iamNilotpal/wal/internal/core/services/segment"
)

const (
	RetentionDays   = 7
	MinSegmentsKept = 2
	MaxSegmentsKept = 10
	Directory       = "./logs"

	DefaultBufferSize = 1024 * 1024 * 1  // 1MB
	MaxBufferSize     = 1024 * 1024 * 16 // 16MB

	CompactInterval = time.Duration(time.Hour * 1)    // 1h
	FlushInterval   = time.Duration(time.Second * 5)  // 5s
	CleanupInterval = time.Duration(time.Minute * 15) // 15m
)

func prepareDefaults(opts *domain.WALOptions) *domain.WALOptions {
	if opts.BufferSize == 0 || opts.BufferSize < DefaultBufferSize {
		opts.BufferSize = DefaultBufferSize
	}

	if opts.BufferSize > MaxBufferSize {
		opts.BufferSize = MaxBufferSize
	}

	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = CleanupInterval
	}

	if opts.CompactInterval == 0 {
		opts.CompactInterval = CompactInterval
	}

	if opts.FlushInterval == 0 {
		opts.FlushInterval = FlushInterval
	}

	if opts.MaxSegmentsKept == 0 {
		opts.MaxSegmentsKept = MaxSegmentsKept
	}

	if opts.MinSegmentsKept == 0 {
		opts.MinSegmentsKept = MinSegmentsKept
	}

	if opts.RetentionDays == 0 {
		opts.RetentionDays = RetentionDays
	}

	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = segment.MinWriteTimeout
	}

	if opts.WriteTimeout != 0 && opts.WriteTimeout > segment.MinWriteTimeout {
		opts.WriteTimeout = segment.MaxWriteTimeout
	}

	if strings.TrimSpace(opts.Directory) == "" {
		opts.Directory = Directory
	}

	if opts.SegmentOptions == nil {
		opts.SegmentOptions = segment.DefaultOptions()
	} else {
		if strings.TrimSpace(opts.SegmentOptions.Directory) == "" {
			opts.SegmentOptions.Directory = segment.SegmentDirectory
		}

		if strings.TrimSpace(opts.SegmentOptions.Prefix) == "" {
			opts.SegmentOptions.Prefix = segment.SegmentPrefix
		}
	}

	if opts.ChecksumOptions == nil {
		opts.ChecksumOptions = checksum.DefaultOptions()
	} else if opts.ChecksumOptions.Enable {
		if strings.TrimSpace(string(opts.ChecksumOptions.Algorithm)) == "" {
			opts.ChecksumOptions.Algorithm = checksum.CRC32IEEE
		}
	}

	if opts.CompressionOptions == nil {
		opts.CompressionOptions = compression.DefaultOptions()
	} else if opts.CompressionOptions.Enable {
		if opts.CompressionOptions.DecoderConcurrency == 0 {
			opts.CompressionOptions.DecoderConcurrency = uint8(runtime.NumCPU())
		}

		if opts.CompressionOptions.EncoderConcurrency == 0 {
			opts.CompressionOptions.EncoderConcurrency = uint8(runtime.NumCPU())
		}
	}

	if opts.PayloadOptions == nil {
		opts.PayloadOptions = config.DefaultPayloadConfig()
	}

	return opts
}
