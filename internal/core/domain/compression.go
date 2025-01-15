package domain

// CompressionOptions configures the compression behavior for WAL segments.
// Compression settings affect both storage efficiency and system performance.
type CompressionOptions struct {
	// Enable toggles compression for rotated WAL segments.
	// When true, segments will be compressed using the specified Level.
	// Compression occurs asynchronously after segment rotation.
	Enable bool

	// Level defines the compression level for zstd when compression is enabled.
	// Supported levels:
	//   - SpeedFastest: Fastest compression, equivalent to zstd's fastest mode
	//   - SpeedDefault: Default balanced compression (≈ zstd level 3)
	//   - SpeedBetterCompression: Better compression ratio (≈ zstd level 7-8) with 2x-3x CPU usage
	//   - SpeedBestCompression: Maximum compression regardless of CPU cost
	// If not specified, SpeedDefault will be used.
	Level uint8

	// EncoderConcurrency specifies the number of concurrent compression operations.
	// Higher values may improve compression speed but increase memory usage.
	// Must be between 1 and 16. Default is number of CPU cores if set to 0.
	EncoderConcurrency uint8

	// DecoderConcurrency specifies the number of concurrent decompression operations.
	// Higher values may improve read performance but increase memory usage.
	// Must be between 1 and 16. Default is number of CPU cores if set to 0.
	DecoderConcurrency uint8
}
