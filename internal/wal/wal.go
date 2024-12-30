package wal

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/wal/pkg/fs"
)

const startSegmentId uint8 = 0
const segmentPrefix = "segment-"

func New(opts *WALOpts) (*WAL, error) {
	context, cancel := context.WithCancel(context.Background())

	if opts.SegmentPrefix == "" {
		opts.SegmentPrefix = segmentPrefix
	}

	wal := WAL{
		segmentSize:    0,
		totalSegment:   0,
		cancelFunc:     cancel,
		ctx:            context,
		log:            opts.Logger,
		logDirName:     opts.LogDirName,
		segmentPrefix:  opts.SegmentPrefix,
		maxLogSegments: opts.MaxLogSegments,
		maxSegmentSize: opts.MaxSegmentSize,
		syncTimer:      time.NewTicker(opts.SyncInterval),
	}

	if err := wal.loadOrCreate(); err != nil {
		return nil, err
	}

	go wal.syncInBackground()
	return &wal, nil
}

func (wal *WAL) loadOrCreate() error {
	// Create the directory, if exists do nothing.
	if err := fs.CreateDir(wal.logDirName); err != nil {
		return err
	}

	// Read all files, only valid if the folder contains any file.
	fileNames, err := fs.ReadFileNames(filepath.Join(wal.logDirName, wal.segmentPrefix+"*"))
	if err != nil {
		return err
	}

	if fileNames == nil || len(fileNames) == 0 {
		file, err := fs.CreateFile(filepath.Join(wal.logDirName, fs.GenerateSegmentName(wal.segmentPrefix, startSegmentId)))
		if err != nil {
			return err
		}

		wal.currSegment = file
		wal.currSegmentId = startSegmentId
		wal.writeBuffer = bufio.NewWriter(file)
		return nil
	}

	id, err := fs.GetLastSegmentId(fileNames, wal.segmentPrefix)
	if err != nil {
		return err
	}

	wal.currSegmentId = id
	fileName := filepath.Join(wal.logDirName, fs.GenerateSegmentName(wal.segmentPrefix, wal.currSegmentId))

	file, err := os.OpenFile(fileName, 0644, os.ModeAppend|os.ModeExclusive)
	if err != nil {
		return err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
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
