package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/wal/pkg/fs"
)

func New(opts *WALOpts) (*WAL, error) {
	context, cancel := context.WithCancel(context.Background())

	if opts.SegmentPrefix == "" {
		opts.SegmentPrefix = segmentPrefix
	}

	wal := WAL{
		cancelFunc:     cancel,
		ctx:            context,
		listeners:      opts.Listeners,
		logDirName:     opts.LogDirName,
		state:          StateInitializing,
		segmentPrefix:  opts.SegmentPrefix,
		maxLogSegments: opts.MaxLogSegments,
		maxSegmentSize: opts.MaxSegmentSize,
		syncInterval:   opts.SyncInterval,
		syncTimer:      time.NewTicker(opts.SyncInterval),
	}

	if wal.listeners == nil {
		wal.listeners = &WalEventListeners{}
	}

	if err := wal.loadOrCreate(); err != nil {
		return nil, err
	}

	wal.state = StateIdle
	go wal.syncInBackground()

	return &wal, nil
}

func (wal *WAL) loadOrCreate() error {
	// Create the directory, if exists do nothing.
	if err := fs.CreateDir(wal.logDirName); err != nil {
		return err
	}

	// Read all log segment files, only valid if the folder contains log files.
	fileNames, err := fs.ReadFileNames(filepath.Join(wal.logDirName, wal.segmentPrefix+"*"))
	if err != nil {
		fmt.Printf("ReadFileNames %+v", err)
		return err
	}

	// If there is no file and create a new file with segment id.
	// Store it and create a new buffered write for that file.
	if fileNames == nil || len(fileNames) == 0 {
		file, err := fs.CreateFile(filepath.Join(wal.logDirName, fs.GenerateSegmentName(wal.segmentPrefix, startSegmentId)))
		if err != nil {
			return err
		}

		wal.totalSegment++
		wal.segmentSize = 0
		wal.lastLSN = startLSN
		wal.currSegmentFile = file
		wal.currSegmentFileId = startSegmentId
		wal.writeBuffer = bufio.NewWriter(file)
		return nil
	}

	latestSegmentId, err := fs.GetLastSegmentId(fileNames, wal.segmentPrefix)
	if err != nil {
		return err
	}

	fileName := filepath.Join(wal.logDirName, fs.GenerateSegmentName(wal.segmentPrefix, latestSegmentId))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	wal.currSegmentFile = file
	wal.segmentSize = uint64(stat.Size())
	wal.currSegmentFileId = latestSegmentId
	wal.writeBuffer = bufio.NewWriter(file)
	wal.totalSegment = uint8(len(fileNames))

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) resetSyncTimer() {
	wal.syncTimer.Reset(wal.syncInterval)
}

func (wal *WAL) syncInBackground() {
	for {
		select {
		case <-wal.syncTimer.C:
			{
				if wal.listeners.OnSyncStart != nil {
					wal.listeners.OnSyncStart(wal.currSegmentFileId, wal.lastLSN, wal.currSegmentFile)
				}

				wal.mutex.Lock()
				wal.state = StateSyncing
				err := wal.autoFsync()
				wal.state = StateIdle
				wal.mutex.Unlock()

				if err != nil && wal.listeners.OnSyncError != nil {
					wal.listeners.OnSyncError(err, wal.currSegmentFile)
				}

				if wal.listeners.OnSyncEnd != nil {
					wal.listeners.OnSyncEnd(wal.currSegmentFileId, wal.lastLSN, wal.currSegmentFile)
				}
			}
		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *WAL) autoFsync() error {
	if err := wal.writeBuffer.Flush(); err != nil {
		return err
	}

	diff := time.Since(wal.lastFsyncedAt)
	if diff >= autoFsyncDuration {
		if err := wal.currSegmentFile.Sync(); err != nil {
			return err
		} else {
			wal.lastFsyncedAt = time.Now()
		}
	}

	wal.resetSyncTimer()
	return nil
}

func (wal *WAL) State() WALState {
	return wal.state
}

func (wal *WAL) Sync(fsync bool) error {
	if err := wal.writeBuffer.Flush(); err != nil {
		return err
	}

	if fsync {
		if err := wal.currSegmentFile.Sync(); err != nil {
			return err
		} else {
			wal.lastFsyncedAt = time.Now()
		}
	}

	wal.resetSyncTimer()
	return nil
}

func (wal *WAL) Close() {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.syncTimer.Stop()
	wal.cancelFunc()
	wal.Sync(true)
}
