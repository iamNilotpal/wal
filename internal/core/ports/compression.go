package ports

// Defines the interface for compression operations.
// This allows us to swap compression algorithms without changing core logic.
type CompressionPort interface {
	// Compress reduces data size.
	// Returns compressed data and any error that occurred.
	Compress(data []byte) ([]byte, error)

	// Decompress restores original data.
	// Returns decompressed data and any error that occurred.
	Decompress(data []byte) ([]byte, error)

	// Close cleans up compression resources.
	Close() error

	// Level returns current compression level.
	Level() uint8
}
