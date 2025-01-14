package ports

// Checksum provides essential checksum operations
type ChecksumPort interface {
	// Calculate returns checksum for given data
	Calculate(data []byte) uint64

	// Verify checks if data matches expected checksum
	Verify(data []byte, expected uint64) bool

	// Size returns the size of the checksum in bytes
	Size() uint8

	// Name returns algorithm identifier
	Name() string
}
