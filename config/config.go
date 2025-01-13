package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WAL              WALConfig `yaml:"wal"`
	StoragePath      string    `yaml:"storage_path"`      // Path to log files
	CompressionLevel uint8     `yaml:"compression_level"` // Compression level (0-9)
	EnableMetrics    bool      `yaml:"enable_metrics"`    // Enable metrics collection
}

// Holds WAL-specific configuration
type WALConfig struct {
	MaxFileSize   uint64        `yaml:"max_file_size"`  // Maximum size before rotation
	MaxFileAge    time.Duration `yaml:"max_file_age"`   // Maximum age before rotation
	BufferSize    uint32        `yaml:"buffer_size"`    // Size of write buffers
	SyncOnWrite   bool          `yaml:"sync_on_write"`  // Sync after each write
	BatchSize     uint16        `yaml:"batch_size"`     // Maximum batch size
	RetentionDays uint8         `yaml:"retention_days"` // Days to retain logs
}

// Returns a Config struct with reasonable default values.
func DefaultConfig() *Config {
	return &Config{
		CompressionLevel: 6,
		EnableMetrics:    true,
		StoragePath:      "/logs",
		WAL: WALConfig{
			RetentionDays: 7,
			SyncOnWrite:   true,
			BatchSize:     1000,
			BufferSize:    1024 * 1024,       // 1MB
			MaxFileSize:   1024 * 1024 * 100, // 100MB
			MaxFileAge:    24 * time.Hour,    // 1 day
		},
	}
}

// Loads configuration from a YAML file.
func LoadConfig(filename string) (*Config, error) {
	// Read the config file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Initialize a new Config struct
	var config Config

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

func validateConfig(config *Config) error {
	if config.StoragePath == "" {
		return fmt.Errorf("storage_path is required")
	}

	if config.CompressionLevel < 0 || config.CompressionLevel > 9 {
		return fmt.Errorf("compression_level must be between 0 and 9")
	}

	if err := validateWALConfig(&config.WAL); err != nil {
		return fmt.Errorf("invalid WAL configuration: %w", err)
	}

	return nil
}

func validateWALConfig(config *WALConfig) error {
	if config.MaxFileSize < 0 {
		return fmt.Errorf("max_file_size must be greater than 0")
	}

	if config.MaxFileAge < 0 {
		return fmt.Errorf("max_file_age must be greater than 0")
	}

	if config.BufferSize < 0 {
		return fmt.Errorf("buffer_size must be greater than 0")
	}

	if config.BatchSize < 0 {
		return fmt.Errorf("batch_size must be greater than 0")
	}

	if config.RetentionDays < 0 {
		return fmt.Errorf("retention_days must be greater than 0")
	}

	return nil
}
