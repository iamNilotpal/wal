package config

// Returns a PayloadSizeConfig with recommended defaults.
func DefaultPayloadConfig() *PayloadConfig {
	return &PayloadConfig{
		MinSize: MinPayloadSize,
		MaxSize: MaxPayloadSize,
	}
}

// Returns config optimized for small, frequent writes.
func DefaultSmallPayloadConfig() *PayloadConfig {
	return &PayloadConfig{
		MaxSize: MaxPayloadSize,
		MinSize: SmallPayloadSize,
	}
}

// Returns config optimized for medium, frequent writes.
func DefaultMediumPayloadConfig() *PayloadConfig {
	return &PayloadConfig{
		MaxSize: MaxPayloadSize,
		MinSize: MediumPayloadSize,
	}
}

// Returns config for bulk operations.
func DefaultLargePayloadConfig() *PayloadConfig {
	return &PayloadConfig{
		MaxSize: MaxPayloadSize,
		MinSize: LargePayloadSize,
	}
}
