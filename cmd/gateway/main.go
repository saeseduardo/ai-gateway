package main

import (
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// TODO: wire up config, providers, and HTTP server in a later module.
	logger.Info("ai-gateway starting")
}
