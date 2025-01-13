package main

import (
	"flag"

	"github.com/iamNilotpal/wal/config"
	"github.com/iamNilotpal/wal/pkg/logger"
)

func main() {
	logger := logger.New("wal-service")
	defer logger.Sync()

	logger.Info("Starting wal service")

	path := flag.String("config", "", "Config file path")
	flag.Parse()

	var cfg *config.Config

	if path == nil || *path == "" {
		cfg = config.DefaultConfig()
		logger.Infow("Using default config", "config", cfg)
	} else {
		c, err := config.LoadConfig(*path)
		if err != nil {
			logger.Fatalw("Error loading config", "path", *path, "error", err)
		}

		cfg = c
		logger.Infow("Config loaded successfully", "config", cfg)
	}
}
