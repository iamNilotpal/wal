package ports

// Defines an interface for calculating and verifying data checksums.
type Checksum interface {
	// Calculates a 32-bit checksum for the provided data.
	// The specific checksum algorithm used depends on the implementation.
	// Returns the calculated checksum value.
	Checksum(data []byte) uint32

	// Validates whether the provided data matches the expected checksum.
	// Returns true if the calculated checksum of the data matches the provided checksum, false otherwise.
	Verify(data []byte, checksum uint32) bool
}
