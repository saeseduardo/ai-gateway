// Package api defines the gateway's public HTTP request/response
// types for the chat completions endpoint, and the mapping between
// them and the internal provider.ChatRequest/ChatResponse/StreamChunk
// types.
//
// These are intentionally kept separate from the provider package's
// types: the HTTP contract mirrors OpenAI's shape for client
// compatibility, while provider types are the gateway's internal,
// provider-agnostic representation. Keeping them distinct means the
// public API can stay stable even as internal provider handling
// evolves.
package api

import "github.com/saeseduardo/ai-gateway/internal/provider"

// ChatMessage is a single message in the public chat completions API.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the public request body for POST /v1/chat/completions.
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature float64       `json:"temperature,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

// ChatChoice is one generated completion within a non-streaming ChatResponse.
type ChatChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

// ChatUsage reports token accounting for a chat completion request.
type ChatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatResponse is the public, non-streaming response body for
// POST /v1/chat/completions, shaped to match OpenAI's chat completion
// response for client compatibility.
type ChatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Model   string       `json:"model"`
	Choices []ChatChoice `json:"choices"`
	Usage   ChatUsage    `json:"usage"`
}

// ChatStreamDelta carries the incremental content of one streaming chunk.
type ChatStreamDelta struct {
	Content string `json:"content,omitempty"`
}

// ChatStreamChoice is one choice within a streaming chunk. FinishReason
// is a pointer so it serializes as JSON null (matching OpenAI's wire
// format) until the final chunk sets it.
type ChatStreamChoice struct {
	Index        int             `json:"index"`
	Delta        ChatStreamDelta `json:"delta"`
	FinishReason *string         `json:"finish_reason"`
}

// ChatStreamChunk is one SSE "data:" payload in the streaming
// response for POST /v1/chat/completions, shaped to match OpenAI's
// chat completion chunk for client compatibility.
type ChatStreamChunk struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Model   string             `json:"model"`
	Choices []ChatStreamChoice `json:"choices"`
}

// ToProviderRequest maps a public ChatRequest to the internal
// provider.ChatRequest passed to a provider.Provider.
func ToProviderRequest(req ChatRequest) *provider.ChatRequest {
	msgs := make([]provider.Message, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = provider.Message{Role: provider.Role(m.Role), Content: m.Content}
	}
	return &provider.ChatRequest{
		Model:       req.Model,
		Messages:    msgs,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stream:      req.Stream,
	}
}

// FromProviderResponse maps a provider.ChatResponse to the public,
// non-streaming ChatResponse.
func FromProviderResponse(resp *provider.ChatResponse) ChatResponse {
	choices := make([]ChatChoice, len(resp.Choices))
	for i, c := range resp.Choices {
		choices[i] = ChatChoice{
			Index:        c.Index,
			Message:      ChatMessage{Role: string(c.Message.Role), Content: c.Message.Content},
			FinishReason: c.FinishReason,
		}
	}
	return ChatResponse{
		ID:      resp.ID,
		Object:  "chat.completion",
		Model:   resp.Model,
		Choices: choices,
		Usage: ChatUsage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		},
	}
}

// FromProviderStreamChunk maps a provider.StreamChunk to one public
// ChatStreamChunk. id identifies every chunk in a given stream (the
// caller generates one per request) and model is the model name the
// client originally requested.
func FromProviderStreamChunk(id, model string, chunk *provider.StreamChunk) ChatStreamChunk {
	choice := ChatStreamChoice{
		Index: 0,
		Delta: ChatStreamDelta{Content: chunk.Delta},
	}
	if chunk.FinishReason != "" {
		fr := chunk.FinishReason
		choice.FinishReason = &fr
	}
	return ChatStreamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Model:   model,
		Choices: []ChatStreamChoice{choice},
	}
}
