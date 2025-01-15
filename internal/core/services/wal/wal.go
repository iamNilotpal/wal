package wal

import (
	"context"

	"github.com/iamNilotpal/wal/internal/core/domain"
	sm "github.com/iamNilotpal/wal/internal/core/services/segment/manager"
)

// WAL implements a Write-Ahead Log for durability and crash recovery.
// It provides ordered, durable storage of log entries by writing them to disk
// before acknowledging writes. The WAL ensures data consistency through
// sequential logging and supports crash recovery.
type WAL struct {
	// Core components and configuration
	options *domain.WALOptions // Configuration controlling WAL behavior
	sm      *sm.SegmentManager // Handles segment lifecycle and maintenance

	// Lifecycle and concurrency control
	ctx    context.Context    // Controls the WAL's operational lifecycle
	cancel context.CancelFunc // Triggers graceful shutdown of WAL operations
}

func New(opts *domain.WALOptions) (*WAL, error) {
	if opts != nil {
		if err := Validate(opts); err != nil {
			return nil, err
		}
	}

	if opts != nil {
		opts = prepareDefaults(opts)
	} else {
		opts = prepareDefaults(&domain.WALOptions{})
	}

	ctx, cancel := context.WithCancel(context.Background())
	sm, err := sm.NewSegmentManager(ctx, opts)
	if err != nil {
		cancel()
		return nil, err
	}

	wal := WAL{
		sm:      sm,
		ctx:     ctx,
		options: opts,
		cancel:  cancel,
	}

	return &wal, nil
}
