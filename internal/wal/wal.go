package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/wal/internal/serialization"
	"github.com/iamNilotpal/wal/pkg/checksum"
	"github.com/iamNilotpal/wal/pkg/fs"
)

// New creates a new instance of the WAL with the provided config.
func New(opts *Config) (*WAL, error) {
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

// Creates logging director with the provided name.
// If the folder exists reads last segment file and stores the required data.
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
	wal.segmentSize = uint32(stat.Size())
	wal.currSegmentFileId = latestSegmentId
	wal.writeBuffer = bufio.NewWriter(file)
	wal.totalSegment = uint8(len(fileNames))

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

// Resets sync interval timer.
func (wal *WAL) resetSyncTimer() {
	wal.syncTimer.Reset(wal.syncInterval)
}

// Syncs data in the background. Optionally responds to events.
func (wal *WAL) syncInBackground() {
	for {
		select {
		case <-wal.syncTimer.C:
			{
				if wal.listeners.OnSyncStart != nil {
					wal.listeners.OnSyncStart(wal.currSegmentFileId, wal.lastLSN, wal.currSegmentFile)
				}

				wal.mutex.Lock()
				wal.state = StateBackgroundSync
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

// Flushes data to the current segment file.
// Optionally syncs the data to disk if the lastFsynced time is older then desired interval.
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

func (wal *WAL) generateWALCommand(data []byte) *WALCommand {
	crcData := append(data, byte(wal.lastLSN))
	sum := checksum.Checksum(crcData)
	return &WALCommand{LSN: wal.lastLSN, Data: crcData, Checksum: sum}
}

func (wal *WAL) writeData(cmd *WALCommand) error {
	data, err := serialization.MarshalJSON(cmd)
	if err != nil {
		return err
	}

	size := uint32(len(data))
	if err := binary.Write(wal.writeBuffer, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err = wal.writeBuffer.Write(data)
	return err
}

// Writes data to wal.
func (wal *WAL) Write(data []byte) error {
	wal.mutex.Lock()
	defer func() {
		wal.state = StateIdle
		wal.mutex.Unlock()
	}()

	wal.lastLSN++
	wal.state = StateWriteData
	cmd := wal.generateWALCommand(data)
	return wal.writeData(cmd)
}

// Retrieves the wal state.
func (wal *WAL) State() WALState {
	return wal.state
}

// Flushes data to the current segment file. Optionally if fsync is true then syncs the data to disk.
func (wal *WAL) Sync(fsync bool) error {
	wal.mutex.Lock()
	defer func() {
		wal.state = StateIdle
		wal.mutex.Unlock()
	}()

	wal.state = StateManualSync
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

// Closes all timers and syncs the buffered data into the disk.
func (wal *WAL) Close() {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.state = StateClosing
	wal.syncTimer.Stop()
	wal.cancelFunc()
	wal.Sync(true)
}
