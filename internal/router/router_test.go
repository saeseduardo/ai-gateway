package router

import (
	"context"
	"errors"
	"testing"

	"github.com/saeseduardo/ai-gateway/internal/provider"
)

// fakeProvider is a minimal provider.Provider stub used only to
// verify Router's model→provider bookkeeping; its methods are never
// invoked by these tests.
type fakeProvider struct {
	name string
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, nil
}

func (f *fakeProvider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
	return nil, nil
}

func TestResolve(t *testing.T) {
	openaiProvider := &fakeProvider{name: "openai"}
	anthropicProvider := &fakeProvider{name: "anthropic"}

	r := New()
	r.Register("gpt-4o", openaiProvider)
	r.Register("claude-sonnet-5", anthropicProvider)

	got, err := r.Resolve("gpt-4o")
	if err != nil {
		t.Fatalf("Resolve(gpt-4o): %v", err)
	}
	if got != provider.Provider(openaiProvider) {
		t.Errorf("Resolve(gpt-4o) = %v, want %v", got, openaiProvider)
	}

	got, err = r.Resolve("claude-sonnet-5")
	if err != nil {
		t.Fatalf("Resolve(claude-sonnet-5): %v", err)
	}
	if got != provider.Provider(anthropicProvider) {
		t.Errorf("Resolve(claude-sonnet-5) = %v, want %v", got, anthropicProvider)
	}
}

func TestResolve_NotFound(t *testing.T) {
	r := New()
	r.Register("gpt-4o", &fakeProvider{name: "openai"})

	_, err := r.Resolve("unknown-model")
	if !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("Resolve(unknown-model) error = %v, want ErrModelNotFound", err)
	}
}
