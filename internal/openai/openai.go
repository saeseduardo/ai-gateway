// Package openai implements provider.Provider against OpenAI's Chat
// Completions API, in both non-streaming and streaming (SSE) modes.
package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/saeseduardo/ai-gateway/internal/config"
	"github.com/saeseduardo/ai-gateway/internal/provider"
)

// name identifies this provider, matching Provider.Name().
const name = "openai"

// Provider implements provider.Provider against the OpenAI Chat
// Completions API.
type Provider struct {
	apiKey  string
	baseURL string

	// httpClient is used for non-streaming requests, where a Timeout
	// bounding the full round trip (connect, send, read the whole
	// body) is appropriate.
	httpClient *http.Client

	// streamHTTPClient is used for streaming requests. It carries no
	// Timeout: an SSE stream can legitimately stay open far longer
	// than a typical request/response timeout, so its lifecycle is
	// governed entirely by the caller's context instead.
	streamHTTPClient *http.Client
}

// New builds a Provider from cfg.
func New(cfg config.ProviderConfig) *Provider {
	return &Provider{
		apiKey:           cfg.APIKey,
		baseURL:          strings.TrimRight(cfg.BaseURL, "/"),
		httpClient:       &http.Client{Timeout: cfg.Timeout},
		streamHTTPClient: &http.Client{},
	}
}

// Name implements provider.Provider.
func (p *Provider) Name() string {
	return name
}

// chatMessage is OpenAI's wire representation of a single message.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest is OpenAI's wire representation of a chat completion
// request, shared by both the streaming and non-streaming paths.
type chatRequest struct {
	Model         string         `json:"model"`
	Messages      []chatMessage  `json:"messages"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
	Temperature   float64        `json:"temperature,omitempty"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

// streamOptions requests that OpenAI include a final usage-only chunk
// in the SSE stream, since usage is otherwise omitted while streaming.
type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// chatResponse is OpenAI's wire representation of a completed
// (non-streaming) chat completion response.
type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int         `json:"index"`
		Message      chatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// errorResponse is OpenAI's wire representation of an error body.
type errorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func toChatMessages(msgs []provider.Message) []chatMessage {
	out := make([]chatMessage, len(msgs))
	for i, m := range msgs {
		out[i] = chatMessage{Role: string(m.Role), Content: m.Content}
	}
	return out
}

// ChatCompletion implements provider.Provider.
func (p *Provider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	body := chatRequest{
		Model:       req.Model,
		Messages:    toChatMessages(req.Messages),
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}

	resp, err := p.doRequest(ctx, body, p.httpClient)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, mapError(resp)
	}

	var out chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("openai: decode response: %w", err)
	}

	choices := make([]provider.Choice, len(out.Choices))
	for i, c := range out.Choices {
		choices[i] = provider.Choice{
			Index: c.Index,
			Message: provider.Message{
				Role:    provider.Role(c.Message.Role),
				Content: c.Message.Content,
			},
			FinishReason: c.FinishReason,
		}
	}

	return &provider.ChatResponse{
		ID:      out.ID,
		Model:   out.Model,
		Choices: choices,
		Usage: provider.Usage{
			PromptTokens:     out.Usage.PromptTokens,
			CompletionTokens: out.Usage.CompletionTokens,
			TotalTokens:      out.Usage.TotalTokens,
		},
	}, nil
}

// ChatCompletionStream implements provider.Provider. It performs the
// upstream request, validates the initial status code, and — once the
// response is confirmed successful — returns a StreamReader that
// parses the SSE body incrementally as Recv is called.
func (p *Provider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
	body := chatRequest{
		Model:         req.Model,
		Messages:      toChatMessages(req.Messages),
		MaxTokens:     req.MaxTokens,
		Temperature:   req.Temperature,
		Stream:        true,
		StreamOptions: &streamOptions{IncludeUsage: true},
	}

	resp, err := p.doRequest(ctx, body, p.streamHTTPClient)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, mapError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	return &streamReader{
		ctx:     ctx,
		body:    resp.Body,
		scanner: scanner,
	}, nil
}

// doRequest marshals body, issues it as a POST bound to ctx (so
// context cancellation aborts the request — including, once headers
// arrive, an in-progress streaming body read), and returns the raw
// response for the caller to interpret.
func (p *Provider) doRequest(ctx context.Context, body chatRequest, client *http.Client) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, &provider.ProviderError{
			Provider:  name,
			Retryable: true,
			Err:       fmt.Errorf("%w: %w", provider.ErrProviderUnavailable, err),
		}
	}
	return resp, nil
}

// mapError reads resp's error body (the caller retains ownership of
// closing resp.Body) and maps its status code to a *provider.ProviderError,
// using the same status→sentinel mapping for both streaming and
// non-streaming requests.
func mapError(resp *http.Response) *provider.ProviderError {
	data, _ := io.ReadAll(resp.Body)

	var parsed errorResponse
	_ = json.Unmarshal(data, &parsed)
	msg := parsed.Error.Message
	if msg == "" {
		msg = strings.TrimSpace(string(data))
	}
	if msg == "" {
		msg = resp.Status
	}

	var sentinel error
	var retryable bool
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		sentinel = provider.ErrRateLimited
		retryable = true
	case resp.StatusCode >= 500:
		sentinel = provider.ErrProviderUnavailable
		retryable = true
	case resp.StatusCode >= 400:
		sentinel = provider.ErrInvalidRequest
		retryable = false
	default:
		sentinel = provider.ErrProviderUnavailable
		retryable = false
	}

	return &provider.ProviderError{
		Provider:   name,
		StatusCode: resp.StatusCode,
		Retryable:  retryable,
		Err:        fmt.Errorf("%w: %s", sentinel, msg),
	}
}

// streamChunk is OpenAI's wire representation of a single SSE "data:"
// payload while streaming.
type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// streamReader implements provider.StreamReader over an OpenAI SSE
// response body. It reads line by line via bufio.Scanner rather than
// buffering the whole body, so chunks are yielded to the caller as
// they arrive.
type streamReader struct {
	ctx     context.Context
	body    io.ReadCloser
	scanner *bufio.Scanner

	// err is the terminal error (or io.EOF) once the stream has ended;
	// once set, every subsequent Recv returns it unchanged.
	err error

	closeOnce sync.Once
	closeErr  error
}

// Recv implements provider.StreamReader.
//
// Every SSE "data:" line that carries choices produces exactly one
// StreamChunk, forwarding whatever content is present — including an
// empty Delta, as happens on OpenAI's first chunk (role-only) and its
// finish-reason chunk (empty delta, non-nil FinishReason). A trailing
// usage-only chunk (enabled via stream_options.include_usage) produces
// a StreamChunk carrying only Usage. Blank lines and non-"data:" lines
// are skipped.
func (r *streamReader) Recv() (*provider.StreamChunk, error) {
	if r.err != nil {
		return nil, r.err
	}

	for r.scanner.Scan() {
		line := r.scanner.Text()
		if line == "" {
			continue
		}
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			r.err = io.EOF
			return nil, io.EOF
		}

		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			r.err = fmt.Errorf("openai: decode stream chunk: %w", err)
			return nil, r.err
		}

		out := &provider.StreamChunk{}
		if len(chunk.Choices) > 0 {
			out.Delta = chunk.Choices[0].Delta.Content
			if chunk.Choices[0].FinishReason != nil {
				out.FinishReason = *chunk.Choices[0].FinishReason
			}
		}
		if chunk.Usage != nil {
			out.Usage = &provider.Usage{
				PromptTokens:     chunk.Usage.PromptTokens,
				CompletionTokens: chunk.Usage.CompletionTokens,
				TotalTokens:      chunk.Usage.TotalTokens,
			}
		}
		return out, nil
	}

	// The scan loop ended without a "[DONE]" sentinel: either the read
	// failed outright (scanner.Err), or the connection was torn down
	// as a side effect of context cancellation, which some transports
	// surface as a clean io.EOF rather than an explicit error — so
	// ctx.Err is checked explicitly before falling back to a normal
	// end-of-stream io.EOF.
	if err := r.scanner.Err(); err != nil {
		r.err = fmt.Errorf("openai: read stream: %w", err)
		return nil, r.err
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return nil, err
	}
	r.err = io.EOF
	return nil, io.EOF
}

// Close implements provider.StreamReader. It is safe to call more
// than once; only the first call's result is returned.
func (r *streamReader) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.body.Close()
	})
	return r.closeErr
}
