package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/iamNilotpal/wal/internal/serialize"
	"github.com/iamNilotpal/wal/pkg/checksum"
	"github.com/iamNilotpal/wal/pkg/fs"
)

// New creates a new instance of the WAL with the provided config.
func New(opts *Config) (*WAL, error) {
	fileSystem := fs.NewLocalFileSystem()
	context, cancel := context.WithCancel(context.Background())

	if opts.SegmentFilePrefix == "" {
		opts.SegmentFilePrefix = segmentPrefix
	}

	wal := WAL{
		totalSegment:            0,
		segmentSize:             0,
		cancelFunc:              cancel,
		context:                 context,
		fs:                      fileSystem,
		activeSegmentId:         startSegmentId,
		logDirectory:            opts.LogDirectory,
		state:                   StateInitializing,
		listeners:               opts.EventListeners,
		maxLogSegments:          opts.MaxLogSegments,
		maxSegmentSize:          opts.MaxSegmentSize,
		segmentPrefix:           opts.SegmentFilePrefix,
		lastLogSequenceNumber:   startLogSequenceNumber,
		oldSegmentDeletionLimit: opts.OldSegmentDeletionLimit,
		syncInterval:            opts.SyncInterval,
		syncTimer:               time.NewTicker(opts.SyncInterval),
	}

	if wal.listeners == nil {
		wal.listeners = &WALEventListeners{}
	}

	if wal.oldSegmentDeletionLimit < 1 {
		wal.oldSegmentDeletionLimit = oldSegmentDeletionLimit
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
	if err := wal.fs.CreateDir(wal.logDirectory, 0750, false); err != nil {
		return err
	}

	// Read all log segment files, only valid if the folder contains log files.
	fileNames, err := wal.fs.ReadDir(filepath.Join(wal.logDirectory, wal.segmentPrefix+"*"))
	if err != nil {
		return err
	}

	// If there is no file and create a new file with segment id.
	// Store it and create a new buffered write for that file.
	if fileNames == nil || len(fileNames) == 0 {
		file, err := wal.fs.CreateFile(
			filepath.Join(wal.logDirectory, wal.generateSegmentName(wal.segmentPrefix, startSegmentId)), false,
		)
		if err != nil {
			return err
		}

		wal.totalSegment++
		wal.segment = file
		wal.buffer = bufio.NewWriter(file)
		return nil
	}

	latestSegmentId, err := wal.getLatestSegmentId(fileNames, wal.segmentPrefix)
	if err != nil {
		return err
	}

	fileName := filepath.Join(wal.logDirectory, wal.generateSegmentName(wal.segmentPrefix, latestSegmentId))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	wal.segment = file
	wal.activeSegmentId = latestSegmentId
	wal.buffer = bufio.NewWriter(file)
	wal.segmentSize = uint32(stat.Size())
	wal.totalSegment = uint8(len(fileNames))

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// TODO: read all the file entires

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
					wal.listeners.OnSyncStart(wal.activeSegmentId, wal.lastLogSequenceNumber, wal.segment)
				}

				wal.mutex.Lock()
				wal.state = StateSyncing
				err := wal.autoFsync()
				wal.state = StateIdle
				wal.mutex.Unlock()

				if err != nil && wal.listeners.OnSyncError != nil {
					wal.listeners.OnSyncError(err, wal.segment)
				}

				if wal.listeners.OnSyncEnd != nil {
					wal.listeners.OnSyncEnd(wal.activeSegmentId, wal.lastLogSequenceNumber, wal.segment)
				}
			}
		case <-wal.context.Done():
			return
		}
	}
}

// Writes data to wal.
func (wal *WAL) Write(data []byte) error {
	wal.mutex.Lock()
	defer func() {
		wal.state = StateIdle
		wal.mutex.Unlock()
	}()

	wal.state = StateWriting

	rotationNeeded, err := wal.checkIfRotationNeeded()
	if err != nil {
		return err
	}

	if rotationNeeded {
		if err := wal.rotateWAL(); err != nil {
			return err
		}
	}

	wal.lastLogSequenceNumber++
	cmd := wal.generateWALCommand(data)
	return wal.writeData(cmd)
}

// Flushes data to the current segment file. If fsync is true then syncs the data to disk.
func (wal *WAL) Sync(fsync bool) error {
	wal.mutex.Lock()
	defer func() {
		wal.state = StateIdle
		wal.mutex.Unlock()
	}()

	wal.state = StateSyncing
	if err := wal.buffer.Flush(); err != nil {
		return err
	}

	if fsync {
		if err := wal.segment.Sync(); err != nil {
			return err
		} else {
			wal.lastFsyncedAt = time.Now()
		}
	}

	wal.resetSyncTimer()
	return nil
}

// Retrieves the wal state.
func (wal *WAL) State() WALState {
	return wal.state
}

// Closes all timers and syncs the buffered data into the disk.
func (wal *WAL) Close() {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.state = StateClosing
	wal.syncTimer.Stop()
	wal.cancelFunc()

	wal.buffer.Flush()
	wal.segment.Sync()
	wal.segment.Close()
}

// Flushes data to the current segment file.
// Optionally syncs the data to disk if the lastFsynced time is older then desired interval.
func (wal *WAL) autoFsync() error {
	if err := wal.buffer.Flush(); err != nil {
		return err
	}

	diff := time.Since(wal.lastFsyncedAt)
	if diff >= autoFsyncDuration {
		if err := wal.segment.Sync(); err != nil {
			return err
		} else {
			wal.lastFsyncedAt = time.Now()
		}
	}

	wal.resetSyncTimer()
	return nil
}

// Generates checksum of the provided data and prepares a WALCommand.
func (wal *WAL) generateWALCommand(data []byte) *WALCommand {
	crcData := append(data, byte(wal.lastLogSequenceNumber))
	sum := checksum.Checksum(crcData)
	return &WALCommand{LogSequenceNumber: wal.lastLogSequenceNumber, Data: crcData, Checksum: sum}
}

// Generates log segment filename. Example: segment-0, segment-1.
func (wal *WAL) generateSegmentName(prefix string, id uint64) string {
	return fmt.Sprintf("%s%d", prefix, id)
}

// Gets the latestSegmentId from log segment names.
func (wal *WAL) getLatestSegmentId(fileNames []string, prefix string) (uint64, error) {
	var lastSegmentId uint64 = 0

	for _, name := range fileNames {
		_, segment := filepath.Split(name)

		id, err := strconv.Atoi(strings.TrimPrefix(segment, prefix))
		if err != nil {
			return 0, err
		}

		segmentId := uint64(id)
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}

	return lastSegmentId, nil
}

func (wal *WAL) writeData(cmd *WALCommand) error {
	data, err := serialize.MarshalJSON(cmd)
	if err != nil {
		return err
	}

	size := uint32(len(data))
	if err := binary.Write(wal.buffer, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err = wal.buffer.Write(data)
	return err
}

func (wal *WAL) checkIfRotationNeeded() (bool, error) {
	stat, err := wal.segment.Stat()
	if err != nil {
		return false, err
	}

	fileSize := uint32(stat.Size() + int64(wal.buffer.Size()))
	if fileSize >= wal.maxSegmentSize {
		return true, nil
	}

	return false, nil
}

func (wal *WAL) rotateWAL() error {
	if err := wal.Sync(true); err != nil {
		return err
	}

	if err := wal.segment.Close(); err != nil {
		return err
	}

	wal.totalSegment++
	wal.activeSegmentId++

	// Delete oldest entries
	if wal.totalSegment > wal.maxLogSegments {
		if err := wal.deleteOldestEntries(); err != nil {
			return err
		}
	}

	// Create a new log segment file
	fileName := filepath.Join(wal.logDirectory, wal.generateSegmentName(wal.segmentPrefix, wal.activeSegmentId))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	wal.segment = file
	wal.segmentSize = 0
	wal.buffer.Reset(file)
	wal.lastLogSequenceNumber = startLogSequenceNumber
	wal.resetSyncTimer()

	return nil
}

func (wal *WAL) deleteOldestEntries() error {
	firstSegmentId := wal.activeSegmentId - uint64(wal.maxLogSegments)

	if wal.maxLogSegments <= 5 {
		path := filepath.Join(wal.logDirectory, wal.generateSegmentName(wal.segmentPrefix, firstSegmentId))
		if err := os.Remove(path); err != nil {
			return err
		}

		wal.totalSegment--
	}

	var count uint8
	for {
		path := filepath.Join(wal.logDirectory, wal.generateSegmentName(wal.segmentPrefix, firstSegmentId))
		if err := os.Remove(path); err != nil {
			return err
		}

		count++
		firstSegmentId++
		wal.totalSegment--

		if count >= wal.oldSegmentDeletionLimit {
			break
		}
	}

	return nil
}
