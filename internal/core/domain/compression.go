package domain

// CompressionOptions configures the compression behavior for WAL segments.
// Compression settings affect both storage efficiency and system performance.
type CompressionOptions struct {
	// Enable toggles compression for rotated WAL segments.
	// When true, segments will be compressed using the specified Level.
	// Compression occurs asynchronously after segment rotation.
	Enable bool

	// Level specifies the compression level (1-9) when Enable is true.
	// - Level 1: Fastest compression, ~30-40% reduction
	// - Level 6: Default, balanced compression, ~60-70% reduction
	// - Level 9: Best compression, ~70-80% reduction, higher CPU usage
	// Must be between 1 and 9. Default is 6.
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
