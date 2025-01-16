package domain

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

// EntryHeader represents the metadata section of a WAL entry.
// It contains all necessary information for validation, sequencing,
// and recovery of log entries. The header is fixed-size and always
// precedes the variable-length payload.
type EntryHeader struct {
	// Timestamp records when the entry was created, in Unix nanoseconds.
	// Used for time-based recovery and debugging.
	Timestamp int64

	// Used to detect corruption and ensure data integrity.
	Checksum uint64

	// PrevOffset points to the start of the previous entry in the log.
	// Enables backward traversal and helps in recovery scenarios.
	PrevOffset uint64

	// Sequence is a monotonically increasing number for each entry.
	// Ensures proper ordering during recovery and helps detect missing entries.
	Sequence uint64

	// PayloadSize stores the exact size of the following payload in bytes.
	// Used to correctly read variable-length payloads and verify integrity.
	PayloadSize uint32

	// Type indicates the kind of operation or data stored in the entry.
	// Different types may have different handling during recovery.
	Type EntryType

	// Compression indicates the algorithm used to compress the payload.
	// 0 means no compression, other values map to specific algorithms.
	Compression uint8

	// Version indicates the format version of the entry structure.
	// Allows for future format changes while maintaining backward compatibility.
	Version uint8
}

// Entry represents a complete WAL entry including both its header and payload.
// Entries are the fundamental unit of storage in the WAL and contain
// both the operation metadata and the actual data to be written.
type Entry struct {
	// Header contains all metadata about the entry including validation,
	// sequencing, and format information. It has a fixed size.
	Header *EntryHeader

	// Payload contains the actual data to be written. Its size is variable
	// and must match PayloadSize in the header. May be compressed according
	// to the Compression field in the header.
	Payload []byte
}

// String returns the string representation of the EntryType.
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
	default:
		return "unknown"
	}
}

// IsValid checks if the EntryType is a known valid type.
// Returns false for any undefined entry types.
func (t EntryType) IsValid() bool {
	return t >= EntryNormal && t <= EntryMetadata
}

// RequiresSync returns true if this entry type requires immediate
// synchronization to disk for consistency guarantees.
func (t EntryType) RequiresSync() bool {
	return t == EntryCheckpoint || t == EntryRotation || t == EntryMetadata
}

// IsSpecial returns true if this is a special entry type that requires
// specific handling during recovery or normal operation.
func (t EntryType) IsSpecial() bool {
	return t != EntryNormal
}
