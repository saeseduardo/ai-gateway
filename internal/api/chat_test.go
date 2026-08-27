package api

import (
	"testing"

	"github.com/saeseduardo/ai-gateway/internal/provider"
)

func TestToProviderRequest_FieldMapping(t *testing.T) {
	tests := []struct {
		name string
		req  ChatRequest
	}{
		{
			name: "single user message, non-streaming",
			req: ChatRequest{
				Model:       "gpt-4o",
				Messages:    []ChatMessage{{Role: "user", Content: "hi"}},
				MaxTokens:   100,
				Temperature: 0.5,
				Stream:      false,
			},
		},
		{
			name: "system + user + assistant, streaming",
			req: ChatRequest{
				Model: "claude-sonnet-5",
				Messages: []ChatMessage{
					{Role: "system", Content: "be concise"},
					{Role: "user", Content: "hi"},
					{Role: "assistant", Content: "hello"},
				},
				MaxTokens:   256,
				Temperature: 0.7,
				Stream:      true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToProviderRequest(tt.req)

			if got.Model != tt.req.Model {
				t.Errorf("Model = %q, want %q", got.Model, tt.req.Model)
			}
			if got.MaxTokens != tt.req.MaxTokens {
				t.Errorf("MaxTokens = %d, want %d", got.MaxTokens, tt.req.MaxTokens)
			}
			if got.Temperature != tt.req.Temperature {
				t.Errorf("Temperature = %v, want %v", got.Temperature, tt.req.Temperature)
			}
			if got.Stream != tt.req.Stream {
				t.Errorf("Stream = %v, want %v", got.Stream, tt.req.Stream)
			}

			if len(got.Messages) != len(tt.req.Messages) {
				t.Fatalf("len(Messages) = %d, want %d", len(got.Messages), len(tt.req.Messages))
			}
			for i, m := range tt.req.Messages {
				if string(got.Messages[i].Role) != m.Role {
					t.Errorf("Messages[%d].Role = %q, want %q", i, got.Messages[i].Role, m.Role)
				}
				if got.Messages[i].Content != m.Content {
					t.Errorf("Messages[%d].Content = %q, want %q", i, got.Messages[i].Content, m.Content)
				}
			}
		})
	}
}

// TestToProviderRequest_PreservesSystemMessage checks specifically that
// a system-role message survives the HTTP -> provider mapping intact,
// at the position it was given. Whether a given provider implementation
// later relocates or merges it is that provider's concern, not this
// mapping's.
func TestToProviderRequest_PreservesSystemMessage(t *testing.T) {
	req := ChatRequest{
		Model: "claude-sonnet-5",
		Messages: []ChatMessage{
			{Role: "system", Content: "you are a helpful assistant"},
			{Role: "user", Content: "hi"},
		},
	}

	got := ToProviderRequest(req)

	if len(got.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2 (system message must be preserved, not dropped or merged)", len(got.Messages))
	}
	if got.Messages[0].Role != provider.RoleSystem {
		t.Errorf("Messages[0].Role = %q, want %q", got.Messages[0].Role, provider.RoleSystem)
	}
	if got.Messages[0].Content != "you are a helpful assistant" {
		t.Errorf("Messages[0].Content = %q, want %q", got.Messages[0].Content, "you are a helpful assistant")
	}
}

func TestFromProviderResponse(t *testing.T) {
	resp := &provider.ChatResponse{
		ID:    "resp-123",
		Model: "gpt-4o",
		Choices: []provider.Choice{
			{
				Index:        0,
				Message:      provider.Message{Role: provider.RoleAssistant, Content: "hello there"},
				FinishReason: "stop",
			},
		},
		Usage: provider.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}

	got := FromProviderResponse(resp)

	if got.ID != resp.ID {
		t.Errorf("ID = %q, want %q", got.ID, resp.ID)
	}
	if got.Object != "chat.completion" {
		t.Errorf("Object = %q, want %q", got.Object, "chat.completion")
	}
	if got.Model != resp.Model {
		t.Errorf("Model = %q, want %q", got.Model, resp.Model)
	}

	if len(got.Choices) != 1 {
		t.Fatalf("len(Choices) = %d, want 1", len(got.Choices))
	}
	choice := got.Choices[0]
	if choice.Index != 0 {
		t.Errorf("Choices[0].Index = %d, want 0", choice.Index)
	}
	if choice.Message.Role != string(provider.RoleAssistant) {
		t.Errorf("Choices[0].Message.Role = %q, want %q", choice.Message.Role, provider.RoleAssistant)
	}
	if choice.Message.Content != "hello there" {
		t.Errorf("Choices[0].Message.Content = %q, want %q", choice.Message.Content, "hello there")
	}
	if choice.FinishReason != "stop" {
		t.Errorf("Choices[0].FinishReason = %q, want %q", choice.FinishReason, "stop")
	}

	if got.Usage.PromptTokens != 10 {
		t.Errorf("Usage.PromptTokens = %d, want 10", got.Usage.PromptTokens)
	}
	if got.Usage.CompletionTokens != 5 {
		t.Errorf("Usage.CompletionTokens = %d, want 5", got.Usage.CompletionTokens)
	}
	if got.Usage.TotalTokens != 15 {
		t.Errorf("Usage.TotalTokens = %d, want 15", got.Usage.TotalTokens)
	}
}

func TestFromProviderResponse_MultipleChoices(t *testing.T) {
	resp := &provider.ChatResponse{
		ID:    "resp-multi",
		Model: "gpt-4o",
		Choices: []provider.Choice{
			{Index: 0, Message: provider.Message{Role: provider.RoleAssistant, Content: "first"}, FinishReason: "stop"},
			{Index: 1, Message: provider.Message{Role: provider.RoleAssistant, Content: "second"}, FinishReason: "length"},
		},
	}

	got := FromProviderResponse(resp)

	if len(got.Choices) != 2 {
		t.Fatalf("len(Choices) = %d, want 2", len(got.Choices))
	}
	if got.Choices[0].Index != 0 || got.Choices[1].Index != 1 {
		t.Errorf("Choices indices = [%d %d], want [0 1]", got.Choices[0].Index, got.Choices[1].Index)
	}
	if got.Choices[1].Message.Content != "second" {
		t.Errorf("Choices[1].Message.Content = %q, want %q", got.Choices[1].Message.Content, "second")
	}
	if got.Choices[1].FinishReason != "length" {
		t.Errorf("Choices[1].FinishReason = %q, want %q", got.Choices[1].FinishReason, "length")
	}
}

// TestChatMapping_RoundTrip is not a literal round trip -- ChatRequest
// and ChatResponse are different shapes -- but it checks that the two
// mapping directions agree on how a message's Role/Content survive a
// trip through provider.Message, by simulating a provider that echoes
// the caller's last message back as its reply.
func TestChatMapping_RoundTrip(t *testing.T) {
	req := ChatRequest{
		Model: "gpt-4o",
		Messages: []ChatMessage{
			{Role: "system", Content: "be terse"},
			{Role: "user", Content: "2+2?"},
		},
	}
	providerReq := ToProviderRequest(req)

	echoed := providerReq.Messages[len(providerReq.Messages)-1]
	providerResp := &provider.ChatResponse{
		ID:    "resp-rt",
		Model: providerReq.Model,
		Choices: []provider.Choice{
			{
				Index:        0,
				Message:      provider.Message{Role: provider.RoleAssistant, Content: echoed.Content},
				FinishReason: "stop",
			},
		},
	}

	got := FromProviderResponse(providerResp)

	wantContent := req.Messages[len(req.Messages)-1].Content
	if got.Choices[0].Message.Content != wantContent {
		t.Errorf("round-trip content = %q, want %q", got.Choices[0].Message.Content, wantContent)
	}
	if got.Model != req.Model {
		t.Errorf("round-trip model = %q, want %q", got.Model, req.Model)
	}
}
