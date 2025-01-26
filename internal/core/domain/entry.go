package domain

import (
	stdErrors "errors"
	"fmt"

	"github.com/iamNilotpal/wal/internal/core/domain/config"
	pb "github.com/iamNilotpal/wal/internal/core/domain/proto"
	"github.com/iamNilotpal/wal/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// System-wide error definitions with detailed context for error handling.
// These errors are designed to provide specific information about failure
// modes to enable appropriate recovery actions.
var (
	// Indicates an entry type value outside the valid range.
	// This typically occurs when reading corrupted data or when encountering
	// entries from a newer version of the WAL format.
	ErrInvalidEntryType = stdErrors.New("invalid entry type")

	// Indicates one of two conditions:
	//  1. Payload size in header exceeds MaxPayloadSize (1GB).
	//  2. Actual payload size doesn't match size recorded in header.
	ErrInvalidPayloadSize = stdErrors.New("invalid payload size")

	// Occurs when attempting to process a nil Entry struct.
	// This is a programming error that should be caught during development.
	ErrNilEntry = stdErrors.New("nil entry")

	// Indicates a nil []byte in EntryPayload.
	// This differs from a valid zero-length payload and indicates
	// potential memory corruption or programming errors.
	ErrNilPayload = stdErrors.New("nil payload")

	// Indicates missing critical metadata fields.
	// Since metadata contains essential fields like checksums and timestamps,
	// operations cannot proceed safely without it.
	ErrNilMetadata = stdErrors.New("nil metadata")

	// Indicates a nil EntryHeader struct.
	// Headers contain critical sequencing and sizing information,
	// so this error must be handled as a critical failure.
	ErrNilHeader = stdErrors.New("nil header")

	// Occurs when an entry has a zero or negative timestamp.
	// Timestamps must be monotonically increasing within a segment and are
	// crucial for crash recovery ordering.
	ErrInvalidTimestamp = stdErrors.New("invalid timestamp")

	// Indicates either:
	//  1. Missing checksum (zero value).
	//  2. Checksum mismatch during validation.
	ErrInvalidChecksum = stdErrors.New("invalid checksum")
)

// EntryType defines the different types of entries that can appear in the WAL.
// Each type has specific handling requirements during normal operation and recovery.
type EntryType uint8

const (
	// EntryNormal represents a standard data entry containing user payload.
	// These entries form the majority of the WAL and contain the actual
	// data modifications that need to be replayed during recovery.
	EntryNormal EntryType = iota + 1

	// EntryCheckpoint marks a consistent state in the WAL where all previous
	// entries have been successfully processed and persisted. Used as a
	// safe starting point during recovery to avoid replaying the entire log.
	EntryCheckpoint

	// EntryRotation indicates that the WAL has started writing to a new segment.
	// Contains metadata about the previous and new segment, facilitating proper
	// segment management during recovery and cleanup operations.
	EntryRotation

	// EntryCompressed indicates that the following payload uses compression.
	// The compression algorithm and level are specified in the entry header.
	// Used to optimize storage while maintaining recoverability.
	EntryCompressed

	// EntryMetadata stores system-level information about the WAL state.
	// May include configuration changes, cleanup markers, or other
	// administrative data needed for proper WAL management.
	EntryMetadata

	// EntrySegmentHeader represents the first entry in a new segment.
	// Contains critical metadata including creation time, and
	// validation information required for segment integrity and recovery.
	EntrySegmentHeader

	// EntrySegmentFinalize indicates the segment has been closed.
	// Stores final segment state including entry count, size stats, and
	// integrity checksums required for validation during recovery.
	EntrySegmentFinalize
)

// Represents the header section of a WAL entry.
// It contains all necessary information for validation, sequencing,
// and recovery of log entries. The header is fixed-size and always
// precedes the variable-length payload.
type EntryHeader struct {
	// Sequence is a monotonically increasing number for each entry.
	// Ensures proper ordering during recovery and helps detect missing entries.
	Sequence uint64

	// PayloadSize stores the exact size of the following payload in bytes.
	// Used to correctly read variable-length payloads and verify integrity.
	PayloadSize uint32

	// Version indicates the format version of the entry structure.
	// Allows for future format changes while maintaining backward compatibility.
	Version uint8
}

// Performs comprehensive header validation to ensure all fields
// meet the WAL's requirements. This includes checking the version compatibility,
// payload size limits, and proper initialization. The validation process is
// critical for maintaining log integrity and preventing corruption.
func (h *EntryHeader) Validate() error {
	if h == nil {
		return errors.NewValidationError("header", nil, ErrNilHeader)
	}

	if h.PayloadSize > config.MaxPayloadSize {
		return errors.NewValidationError("payloadSize", h.PayloadSize, ErrInvalidPayloadSize)
	}

	if h.Version < config.MinVersion || h.Version > config.MaxVersion {
		return errors.NewValidationError("version", h.Version, fmt.Errorf("invalid version: %d", h.Version))
	}

	return nil
}

// Contains per-entry metadata to support durability,
// recovery, and debugging capabilities. This metadata is critical for
// maintaining log integrity and enabling efficient log operations.
type PayloadMetadata struct {
	// Timestamp records when the entry was created, in Unix nanoseconds.
	// Used for time-based recovery and debugging.
	Timestamp int64

	// Used to detect corruption and ensure data integrity.
	Checksum uint64

	// PrevOffset points to the start of the previous entry in the log.
	// Enables backward traversal and helps in recovery scenarios.
	PrevOffset uint64

	// Type indicates the kind of operation or data stored in the entry.
	// Different types may have different handling during recovery.
	Type EntryType
}

// Performs comprehensive validation of metadata fields according to
// strict rules required for WAL consistency. The validation ensures timestamp
// monotonicity, checksum presence, and entry type validity. A nil metadata
// structure is considered an unrecoverable error since metadata is essential
// for log integrity and recovery operations.
func (m *PayloadMetadata) Validate() error {
	if m == nil {
		return errors.NewValidationError("metadata", nil, ErrNilMetadata)
	}

	if !m.Type.IsValid() {
		return errors.NewValidationError("type", m.Type, ErrInvalidEntryType)
	}

	if m.Timestamp <= 0 {
		return errors.NewValidationError("timestamp", m.Timestamp, ErrInvalidTimestamp)
	}

	if m.Checksum == 0 {
		return errors.NewValidationError("checksum", m.Checksum, ErrInvalidChecksum)
	}

	return nil
}

// Encapsulates the actual data being written along with its associated metadata.
type EntryPayload struct {
	// Payload contains the raw bytes to be written to the log.
	Payload []byte
	// Metadata points to additional information about this payload
	// including timestamps, checksums, and traversal pointers.
	Metadata *PayloadMetadata
}

// Performs a comprehensive check of the payload structure and its
// contents. The validation process ensures that both the raw payload data
// and associated metadata meet all requirements for safe WAL operation.
// This includes verification of size constraints, metadata validity, and
// proper initialization of all required fields.
func (p *EntryPayload) Validate() error {
	if p == nil {
		return errors.NewValidationError("payload", nil, ErrNilPayload)
	}
	if p.Payload == nil {
		return errors.NewValidationError("payload", nil, ErrNilPayload)
	}
	return p.Metadata.Validate()
}

// Entry represents a complete WAL entry including both its header and payload.
// Entries are the fundamental unit of storage in the WAL and contain
// both the operation metadata and the actual data to be written.
type Entry struct {
	// Header contains all metadata about the entry including validation,
	// sequencing, and format information. It has a fixed size.
	Header *EntryHeader

	// Payload contains the actual data to be written. Its size is variable
	// and must match PayloadSize in the header.
	Payload *EntryPayload
}

// Performs a comprehensive validation of the entire entry structure.
// This includes checking the header, payload, and ensuring consistency between
// them. The validation process verifies structural integrity, size matching,
// and proper initialization of all components. This method must be called
// before any entry is written to the WAL to maintain system integrity.
func (e *Entry) Validate() error {
	if e == nil {
		return ErrNilEntry
	}

	if err := e.Header.Validate(); err != nil {
		return err
	}

	if err := e.Payload.Validate(); err != nil {
		return err
	}

	return nil
}

// Converts an EntryType to its string representation for logging
// and debugging purposes. Each entry type has a unique string identifier
// that clearly indicates its purpose and role in the WAL system. Unknown
// entry types are reported with their numeric value to aid in troubleshooting.
func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryCheckpoint:
		return "checkpoint"
	case EntryRotation:
		return "rotation"
	case EntryCompressed:
		return "compressed"
	case EntryMetadata:
		return "metadata"
	case EntrySegmentHeader:
		return "segment_header"
	case EntrySegmentFinalize:
		return "segment_finalize"
	default:
		return fmt.Sprintf("unknown - (%d)", t)
	}
}

// Checks if an EntryType value is within the defined range of valid types.
func (t EntryType) IsValid() bool {
	return t >= EntryNormal && t <= EntrySegmentFinalize
}

// Determines whether an entry type requires immediate synchronization
// to disk. This decision is critical for maintaining consistency guarantees
// and ensuring proper recovery capabilities. Entries requiring sync must be
// written to disk before any dependent  operations can proceed.
func (t EntryType) RequiresSync() bool {
	switch t {
	case EntryCheckpoint, EntryRotation, EntryMetadata,
		EntrySegmentHeader, EntrySegmentFinalize:
		return true
	default:
		return false
	}
}

// Identifies entry types that require special handling during
// normal operation or recovery. Special entries typically contain system
// metadata or control information rather than user data. This distinction
// is important for proper entry processing and recovery sequencing.
func (t EntryType) IsSpecial() bool {
	return t != EntryNormal
}

// Serializes an entry to its Protocol Buffer representation.
func (e *Entry) MarshalProto(validate bool) ([]byte, error) {
	if validate {
		if err := e.Validate(); err != nil {
			return nil, err
		}
	}

	entry := pb.Entry{
		Payload: e.Payload.Payload,
		Metadata: &pb.PayloadMetadata{
			Timestamp:  int64(e.Payload.Metadata.Timestamp),
			Checksum:   uint64(e.Payload.Metadata.Checksum),
			PrevOffset: uint64(e.Payload.Metadata.PrevOffset),
			Type:       pb.EntryType(e.Payload.Metadata.Type),
		},
	}

	// Deterministic marshaling for consistent byte representation.
	opts := proto.MarshalOptions{Deterministic: true}

	data, err := opts.Marshal(&entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto : %w", err)
	}

	return data, nil
}

// Deserializes an entry from its Protocol Buffer representation.
func (e *Entry) UnMarshalProto(data []byte) error {
	if len(data) == 0 {
		return errors.NewValidationError("proto", nil, fmt.Errorf("empty proto data"))
	}

	var entry pb.Entry
	opts := proto.UnmarshalOptions{DiscardUnknown: true}

	if err := opts.Unmarshal(data, &entry); err != nil {
		return fmt.Errorf("failed to unmarshal proto: %w", err)
	}

	if entry.Payload == nil {
		return ErrNilPayload
	}

	if entry.Metadata == nil {
		return ErrNilMetadata
	}

	e.Payload.Payload = entry.Payload
	e.Payload.Metadata.Checksum = entry.Metadata.Checksum
	e.Payload.Metadata.Timestamp = entry.Metadata.Timestamp
	e.Payload.Metadata.Type = EntryType(entry.Metadata.Type)
	e.Payload.Metadata.PrevOffset = entry.Metadata.PrevOffset

	return e.Validate()
}
