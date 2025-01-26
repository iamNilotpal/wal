package config

// Returns a PayloadSizeConfig with recommended defaults.
func DefaultPayloadConfig() *PayloadOptions {
	return &PayloadOptions{
		MinSize: MinPayloadSize,
		MaxSize: MaxPayloadSize,
	}
}
