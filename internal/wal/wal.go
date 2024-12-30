package wal

import (
	"context"
	"time"

	"github.com/iamNilotpal/wal/pkg/fs"
)

func New(opts *WALOpts) (*WAL, error) {
	context, cancel := context.WithCancel(context.Background())

	wal := WAL{
		logDir:         opts.LogDir,
		log:            opts.Logger,
		maxSegments:    opts.MaxSegments,
		mazSegmentSize: opts.MaxSegmentSize,
		ctx:            context,
		cancelFunc:     cancel,
		syncTimer:      time.NewTicker(opts.SyncInterval),
	}

	if err := fs.MustCreateDir(opts.LogDir); err != nil {
		return nil, err
	}

	go wal.syncInBackground()

	return &wal, nil
}

func (wal *WAL) syncInBackground() {
	for {
		select {
		case <-wal.syncTimer.C:
			{
				wal.mutex.Lock()
				if err := wal.currSegment.Sync(); err != nil {
					return
				}
				wal.mutex.Unlock()
			}
		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *WAL) Close() {
	wal.log.Infoln("closing wal")
	wal.syncTimer.Stop()
	wal.cancelFunc()

	wal.mutex.Lock()
	wal.currSegment.Sync()
	wal.mutex.Unlock()
}
