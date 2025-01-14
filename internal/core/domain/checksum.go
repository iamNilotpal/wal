package domain

import (
	"github.com/iamNilotpal/wal/internal/core/ports"
)

// ChecksumAlgorithm represents supported checksum algorithms
type ChecksumAlgorithm string

// ChecksumOptions defines configuration for segment checksum.
type ChecksumOptions struct {
	// Enable controls whether checksum verification is active.
	// When true, checksums are calculated and verified during I/O operations.
	// When false, no checksums are calculated or verified, offering better
	// performance at the cost of reduced data integrity guarantees.
	//
	// Default: true
	Enable bool

	// Algorithm specifies which checksum algorithm to use.
	// Defaults to CRC32IEEE if not specified.
	Algorithm ChecksumAlgorithm

	// Custom allows using a custom ChecksumPort implementation.
	// If provided, it takes precedence over Algorithm.
	Custom ports.ChecksumPort

	// VerifyOnRead determines if checksums should be verified during read operations.
	// Recommended to keep enabled except in specific performance-critical scenarios.
	// Default: true
	VerifyOnRead bool

	// VerifyOnWrite determines if checksums should be calculated and stored during writes.
	// Disabling allows faster writes but removes corruption detection.
	// Default: true
	VerifyOnWrite bool
}
