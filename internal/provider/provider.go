// Package provider defines the gateway's normalized abstraction over
// upstream LLM providers (OpenAI, Anthropic, ...): the Provider
// interface, request/response and streaming types, and typed errors.
// It intentionally contains no concrete provider implementations —
// those live in their own packages and satisfy the Provider interface
// defined here.
package provider

import "context"

// Provider is implemented by every upstream LLM backend the gateway
// can route requests to (e.g. OpenAI, Anthropic). Implementations are
// responsible for translating between the gateway's normalized types
// and the provider's native API.
type Provider interface {
	// Name returns the provider's identifier (e.g. "openai", "anthropic").
	// It is stable and suitable for logging, metrics labels, and routing.
	Name() string

	// ChatCompletion performs a single non-streaming chat completion
	// request and returns the full response once it is available.
	ChatCompletion(ctx context.Context, req *ChatRequest) (*ChatResponse, error)

	// ChatCompletionStream performs a streaming chat completion
	// request. It returns a StreamReader that yields incremental
	// StreamChunk values as the upstream response arrives; the caller
	// must call Close on the returned StreamReader once done.
	ChatCompletionStream(ctx context.Context, req *ChatRequest) (StreamReader, error)
}
