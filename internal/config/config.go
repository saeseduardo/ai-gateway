// Package config loads typed application configuration for the AI
// Gateway exclusively from environment variables (prefix GATEWAY_),
// applying sensible defaults and failing fast on missing or malformed
// values.
package config

import (
	"errors"
	"time"
)

// Environment variable names, grouped by the sub-config they belong to.
const (
	envServerPort            = "GATEWAY_SERVER_PORT"
	envServerReadTimeout     = "GATEWAY_SERVER_READ_TIMEOUT"
	envServerWriteTimeout    = "GATEWAY_SERVER_WRITE_TIMEOUT"
	envServerIdleTimeout     = "GATEWAY_SERVER_IDLE_TIMEOUT"
	envServerShutdownTimeout = "GATEWAY_SERVER_SHUTDOWN_TIMEOUT"

	envOpenAIAPIKey  = "GATEWAY_OPENAI_API_KEY"
	envOpenAIBaseURL = "GATEWAY_OPENAI_BASE_URL"
	envOpenAITimeout = "GATEWAY_OPENAI_TIMEOUT"

	envAnthropicAPIKey  = "GATEWAY_ANTHROPIC_API_KEY"
	envAnthropicBaseURL = "GATEWAY_ANTHROPIC_BASE_URL"
	envAnthropicTimeout = "GATEWAY_ANTHROPIC_TIMEOUT"

	envRedisAddress  = "GATEWAY_REDIS_ADDRESS"
	envRedisPassword = "GATEWAY_REDIS_PASSWORD"
	envRedisDB       = "GATEWAY_REDIS_DB"

	envTelemetryMetricsPort = "GATEWAY_TELEMETRY_METRICS_PORT"
)

// Default values applied when the corresponding environment variable
// is unset or empty. There are no defaults for the provider API keys:
// those are secrets and must be supplied explicitly.
const (
	defaultServerPort            = 8080
	defaultServerReadTimeout     = 15 * time.Second
	defaultServerWriteTimeout    = 15 * time.Second
	defaultServerIdleTimeout     = 60 * time.Second
	defaultServerShutdownTimeout = 10 * time.Second

	defaultOpenAIBaseURL = "https://api.openai.com/v1"
	defaultOpenAITimeout = 30 * time.Second

	defaultAnthropicBaseURL = "https://api.anthropic.com"
	defaultAnthropicTimeout = 30 * time.Second

	defaultRedisAddress = "localhost:6379"
	defaultRedisDB      = 0

	defaultTelemetryMetricsPort = 9090
)

// Config is the fully resolved, typed configuration for the gateway
// process. It is built once at startup by Load.
type Config struct {
	// Server holds the HTTP server's listening and timeout settings.
	Server ServerConfig
	// Providers holds per-provider connection settings (OpenAI, Anthropic).
	Providers ProvidersConfig
	// Redis holds the connection settings for the Redis instance used
	// by the gateway (e.g. for rate limiting or caching in later modules).
	Redis RedisConfig
	// Telemetry holds settings for metrics/observability endpoints.
	Telemetry TelemetryConfig
}

// ServerConfig configures the gateway's HTTP server.
type ServerConfig struct {
	// Port is the TCP port the HTTP server listens on.
	Port int
	// ReadTimeout is the maximum duration for reading an entire request,
	// including the body (net/http.Server.ReadTimeout).
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration before timing out writes of
	// the response (net/http.Server.WriteTimeout).
	WriteTimeout time.Duration
	// IdleTimeout is the maximum amount of time to wait for the next
	// request when keep-alives are enabled (net/http.Server.IdleTimeout).
	IdleTimeout time.Duration
	// ShutdownTimeout is the maximum duration to wait for in-flight
	// requests to complete during a graceful shutdown.
	ShutdownTimeout time.Duration
}

// ProviderConfig configures a single upstream LLM provider.
type ProviderConfig struct {
	// APIKey authenticates requests to the provider. Required; has no
	// default value since it is a secret.
	APIKey string
	// BaseURL is the root URL of the provider's API.
	BaseURL string
	// Timeout is the maximum duration allowed for a single request to
	// the provider.
	Timeout time.Duration
}

// ProvidersConfig groups the configuration for every upstream LLM
// provider the gateway talks to.
type ProvidersConfig struct {
	// OpenAI holds connection settings for the OpenAI API.
	OpenAI ProviderConfig
	// Anthropic holds connection settings for the Anthropic API.
	Anthropic ProviderConfig
}

// RedisConfig configures the connection to the Redis instance used by
// the gateway.
type RedisConfig struct {
	// Address is the Redis server address in "host:port" form.
	Address string
	// Password authenticates against the Redis server. Empty means no
	// authentication.
	Password string
	// DB is the Redis logical database number to select.
	DB int
}

// TelemetryConfig configures the gateway's observability endpoints.
type TelemetryConfig struct {
	// MetricsPort is the TCP port the metrics endpoint (e.g. /metrics)
	// listens on.
	MetricsPort int
}

// Load builds a Config by reading environment variables (prefix
// GATEWAY_), falling back to sensible defaults where a variable is
// unset. It fails fast: malformed values are collected and returned
// as a single joined error, and required fields (the provider API
// keys) are checked via Validate before the Config is returned.
func Load() (*Config, error) {
	var errs []error

	serverPort, err := getEnvInt(envServerPort, defaultServerPort)
	errs = append(errs, err)
	serverReadTimeout, err := getEnvDuration(envServerReadTimeout, defaultServerReadTimeout)
	errs = append(errs, err)
	serverWriteTimeout, err := getEnvDuration(envServerWriteTimeout, defaultServerWriteTimeout)
	errs = append(errs, err)
	serverIdleTimeout, err := getEnvDuration(envServerIdleTimeout, defaultServerIdleTimeout)
	errs = append(errs, err)
	serverShutdownTimeout, err := getEnvDuration(envServerShutdownTimeout, defaultServerShutdownTimeout)
	errs = append(errs, err)

	openAITimeout, err := getEnvDuration(envOpenAITimeout, defaultOpenAITimeout)
	errs = append(errs, err)

	anthropicTimeout, err := getEnvDuration(envAnthropicTimeout, defaultAnthropicTimeout)
	errs = append(errs, err)

	redisDB, err := getEnvInt(envRedisDB, defaultRedisDB)
	errs = append(errs, err)

	telemetryMetricsPort, err := getEnvInt(envTelemetryMetricsPort, defaultTelemetryMetricsPort)
	errs = append(errs, err)

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	cfg := &Config{
		Server: ServerConfig{
			Port:            serverPort,
			ReadTimeout:     serverReadTimeout,
			WriteTimeout:    serverWriteTimeout,
			IdleTimeout:     serverIdleTimeout,
			ShutdownTimeout: serverShutdownTimeout,
		},
		Providers: ProvidersConfig{
			OpenAI: ProviderConfig{
				APIKey:  getEnv(envOpenAIAPIKey, ""),
				BaseURL: getEnv(envOpenAIBaseURL, defaultOpenAIBaseURL),
				Timeout: openAITimeout,
			},
			Anthropic: ProviderConfig{
				APIKey:  getEnv(envAnthropicAPIKey, ""),
				BaseURL: getEnv(envAnthropicBaseURL, defaultAnthropicBaseURL),
				Timeout: anthropicTimeout,
			},
		},
		Redis: RedisConfig{
			Address:  getEnv(envRedisAddress, defaultRedisAddress),
			Password: getEnv(envRedisPassword, ""),
			DB:       redisDB,
		},
		Telemetry: TelemetryConfig{
			MetricsPort: telemetryMetricsPort,
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that all required configuration is present. It is
// called by Load, but is exported so callers can re-validate a Config
// built or mutated outside of Load (e.g. in tests).
func (c *Config) Validate() error {
	var errs []error

	if c.Providers.OpenAI.APIKey == "" {
		errs = append(errs, errors.New(envOpenAIAPIKey+" is required"))
	}
	if c.Providers.Anthropic.APIKey == "" {
		errs = append(errs, errors.New(envAnthropicAPIKey+" is required"))
	}

	return errors.Join(errs...)
}
