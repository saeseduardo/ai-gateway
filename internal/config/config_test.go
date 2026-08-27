package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_AllValid(t *testing.T) {
	t.Setenv(envOpenAIAPIKey, "sk-openai-test")
	t.Setenv(envAnthropicAPIKey, "sk-anthropic-test")
	t.Setenv(envServerPort, "9000")
	t.Setenv(envServerReadTimeout, "5s")
	t.Setenv(envServerWriteTimeout, "10s")
	t.Setenv(envServerIdleTimeout, "30s")
	t.Setenv(envServerShutdownTimeout, "2s")
	t.Setenv(envOpenAIBaseURL, "https://openai.example/v1")
	t.Setenv(envOpenAITimeout, "20s")
	t.Setenv(envAnthropicBaseURL, "https://anthropic.example")
	t.Setenv(envAnthropicTimeout, "25s")
	t.Setenv(envRedisAddress, "redis.example:6380")
	t.Setenv(envRedisPassword, "s3cret")
	t.Setenv(envRedisDB, "3")
	t.Setenv(envTelemetryMetricsPort, "9999")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	if cfg.Server.Port != 9000 {
		t.Errorf("Server.Port = %d, want 9000", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 5*time.Second {
		t.Errorf("Server.ReadTimeout = %v, want 5s", cfg.Server.ReadTimeout)
	}
	if cfg.Providers.OpenAI.APIKey != "sk-openai-test" {
		t.Errorf("Providers.OpenAI.APIKey = %q, want %q", cfg.Providers.OpenAI.APIKey, "sk-openai-test")
	}
	if cfg.Providers.OpenAI.BaseURL != "https://openai.example/v1" {
		t.Errorf("Providers.OpenAI.BaseURL = %q, want %q", cfg.Providers.OpenAI.BaseURL, "https://openai.example/v1")
	}
	if cfg.Providers.Anthropic.APIKey != "sk-anthropic-test" {
		t.Errorf("Providers.Anthropic.APIKey = %q, want %q", cfg.Providers.Anthropic.APIKey, "sk-anthropic-test")
	}
	if cfg.Providers.Anthropic.Timeout != 25*time.Second {
		t.Errorf("Providers.Anthropic.Timeout = %v, want 25s", cfg.Providers.Anthropic.Timeout)
	}
	if cfg.Redis.Address != "redis.example:6380" {
		t.Errorf("Redis.Address = %q, want %q", cfg.Redis.Address, "redis.example:6380")
	}
	if cfg.Redis.DB != 3 {
		t.Errorf("Redis.DB = %d, want 3", cfg.Redis.DB)
	}
	if cfg.Telemetry.MetricsPort != 9999 {
		t.Errorf("Telemetry.MetricsPort = %d, want 9999", cfg.Telemetry.MetricsPort)
	}
}

func TestLoad_MissingOpenAIKey(t *testing.T) {
	t.Setenv(envOpenAIAPIKey, "")
	t.Setenv(envAnthropicAPIKey, "sk-anthropic-test")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error for missing OpenAI API key")
	}
	if !strings.Contains(err.Error(), envOpenAIAPIKey) {
		t.Errorf("Load() error = %q, want it to mention %q", err.Error(), envOpenAIAPIKey)
	}
}

func TestLoad_MissingAnthropicKey(t *testing.T) {
	t.Setenv(envOpenAIAPIKey, "sk-openai-test")
	t.Setenv(envAnthropicAPIKey, "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error for missing Anthropic API key")
	}
	if !strings.Contains(err.Error(), envAnthropicAPIKey) {
		t.Errorf("Load() error = %q, want it to mention %q", err.Error(), envAnthropicAPIKey)
	}
}

// TestLoad_MalformedDuration documents Load's actual fail-fast contract
// (see the package doc comment): a malformed duration is a hard error,
// not a silent fallback to the default.
func TestLoad_MalformedDuration(t *testing.T) {
	t.Setenv(envOpenAIAPIKey, "sk-openai-test")
	t.Setenv(envAnthropicAPIKey, "sk-anthropic-test")
	t.Setenv(envServerReadTimeout, "abc")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error for malformed duration")
	}
	if !strings.Contains(err.Error(), envServerReadTimeout) {
		t.Errorf("Load() error = %q, want it to mention %q", err.Error(), envServerReadTimeout)
	}
}

func TestLoad_DefaultsWhenUnset(t *testing.T) {
	t.Setenv(envOpenAIAPIKey, "sk-openai-test")
	t.Setenv(envAnthropicAPIKey, "sk-anthropic-test")
	t.Setenv(envServerPort, "")
	t.Setenv(envServerReadTimeout, "")
	t.Setenv(envServerWriteTimeout, "")
	t.Setenv(envServerIdleTimeout, "")
	t.Setenv(envServerShutdownTimeout, "")
	t.Setenv(envOpenAIBaseURL, "")
	t.Setenv(envOpenAITimeout, "")
	t.Setenv(envAnthropicBaseURL, "")
	t.Setenv(envAnthropicTimeout, "")
	t.Setenv(envRedisAddress, "")
	t.Setenv(envRedisDB, "")
	t.Setenv(envTelemetryMetricsPort, "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	if cfg.Server.Port != defaultServerPort {
		t.Errorf("Server.Port = %d, want default %d", cfg.Server.Port, defaultServerPort)
	}
	if cfg.Server.ReadTimeout != defaultServerReadTimeout {
		t.Errorf("Server.ReadTimeout = %v, want default %v", cfg.Server.ReadTimeout, defaultServerReadTimeout)
	}
	if cfg.Server.WriteTimeout != defaultServerWriteTimeout {
		t.Errorf("Server.WriteTimeout = %v, want default %v", cfg.Server.WriteTimeout, defaultServerWriteTimeout)
	}
	if cfg.Server.IdleTimeout != defaultServerIdleTimeout {
		t.Errorf("Server.IdleTimeout = %v, want default %v", cfg.Server.IdleTimeout, defaultServerIdleTimeout)
	}
	if cfg.Server.ShutdownTimeout != defaultServerShutdownTimeout {
		t.Errorf("Server.ShutdownTimeout = %v, want default %v", cfg.Server.ShutdownTimeout, defaultServerShutdownTimeout)
	}
	if cfg.Providers.OpenAI.BaseURL != defaultOpenAIBaseURL {
		t.Errorf("Providers.OpenAI.BaseURL = %q, want default %q", cfg.Providers.OpenAI.BaseURL, defaultOpenAIBaseURL)
	}
	if cfg.Providers.OpenAI.Timeout != defaultOpenAITimeout {
		t.Errorf("Providers.OpenAI.Timeout = %v, want default %v", cfg.Providers.OpenAI.Timeout, defaultOpenAITimeout)
	}
	if cfg.Providers.Anthropic.BaseURL != defaultAnthropicBaseURL {
		t.Errorf("Providers.Anthropic.BaseURL = %q, want default %q", cfg.Providers.Anthropic.BaseURL, defaultAnthropicBaseURL)
	}
	if cfg.Redis.Address != defaultRedisAddress {
		t.Errorf("Redis.Address = %q, want default %q", cfg.Redis.Address, defaultRedisAddress)
	}
	if cfg.Redis.DB != defaultRedisDB {
		t.Errorf("Redis.DB = %d, want default %d", cfg.Redis.DB, defaultRedisDB)
	}
	if cfg.Telemetry.MetricsPort != defaultTelemetryMetricsPort {
		t.Errorf("Telemetry.MetricsPort = %d, want default %d", cfg.Telemetry.MetricsPort, defaultTelemetryMetricsPort)
	}
}

// TestGetEnvDuration exercises the helper directly: unset/empty falls
// back to the default, a valid value parses, and a malformed value is
// reported as an error rather than silently falling back.
func TestGetEnvDuration(t *testing.T) {
	const key = "GATEWAY_TEST_DURATION_XYZ"
	const fallback = 7 * time.Second

	tests := []struct {
		name    string
		setEnv  bool
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses default", setEnv: false, want: fallback},
		{name: "empty uses default", setEnv: true, value: "", want: fallback},
		{name: "valid duration parses", setEnv: true, value: "250ms", want: 250 * time.Millisecond},
		{name: "malformed value errors", setEnv: true, value: "not-a-duration", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setEnv {
				t.Setenv(key, tt.value)
			}

			got, err := getEnvDuration(key, fallback)

			if tt.wantErr {
				if err == nil {
					t.Fatal("getEnvDuration() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("getEnvDuration() error = %v, want nil", err)
			}
			if got != tt.want {
				t.Errorf("getEnvDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErr    bool
		wantErrHas []string
	}{
		{
			name: "both keys present",
			cfg: Config{Providers: ProvidersConfig{
				OpenAI:    ProviderConfig{APIKey: "sk-openai"},
				Anthropic: ProviderConfig{APIKey: "sk-anthropic"},
			}},
			wantErr: false,
		},
		{
			name:       "both keys missing",
			cfg:        Config{},
			wantErr:    true,
			wantErrHas: []string{envOpenAIAPIKey, envAnthropicAPIKey},
		},
		{
			name: "only anthropic key missing",
			cfg: Config{Providers: ProvidersConfig{
				OpenAI: ProviderConfig{APIKey: "sk-openai"},
			}},
			wantErr:    true,
			wantErrHas: []string{envAnthropicAPIKey},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
			for _, want := range tt.wantErrHas {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Validate() error = %q, want it to contain %q", err.Error(), want)
				}
			}
		})
	}
}
