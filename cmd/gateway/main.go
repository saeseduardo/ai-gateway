package main

import (
	"log/slog"
	"os"

	"github.com/saeseduardo/ai-gateway/internal/anthropic"
	"github.com/saeseduardo/ai-gateway/internal/config"
	"github.com/saeseduardo/ai-gateway/internal/openai"
	"github.com/saeseduardo/ai-gateway/internal/router"
	"github.com/saeseduardo/ai-gateway/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	openaiProvider := openai.New(cfg.Providers.OpenAI)
	anthropicProvider := anthropic.New(cfg.Providers.Anthropic)

	// The model→provider mapping is hardcoded here for now; a later
	// module will move it into config so models can be added or
	// removed without a rebuild.
	rt := router.New()
	for _, model := range []string{"gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"} {
		rt.Register(model, openaiProvider)
	}
	for _, model := range []string{"claude-sonnet-5", "claude-opus-5", "claude-haiku-4-5-20251001"} {
		rt.Register(model, anthropicProvider)
	}

	srv := server.New(cfg.Server, logger)
	srv.RegisterChatRoutes(rt)

	if err := srv.Run(); err != nil {
		logger.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}
