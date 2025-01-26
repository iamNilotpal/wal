package config

// Returns a PayloadSizeConfig with recommended defaults.
func DefaultPayloadConfig() *PayloadConfig {
	return &PayloadConfig{
		MinSize: MinPayloadSize,
		MaxSize: MaxPayloadSize,
	}
}
