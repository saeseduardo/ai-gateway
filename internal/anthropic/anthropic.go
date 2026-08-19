// Package anthropic implements provider.Provider against Anthropic's
// Messages API, in both non-streaming and streaming (SSE) modes.
package anthropic

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
const name = "anthropic"

// apiVersion is the Anthropic API version this client speaks, sent on
// every request via the anthropic-version header.
const apiVersion = "2023-06-01"

// Provider implements provider.Provider against the Anthropic
// Messages API.
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

// message is Anthropic's wire representation of a single message.
// Unlike OpenAI, system instructions are not a message role: they are
// carried on chatRequest.System instead.
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest is Anthropic's wire representation of a messages
// request, shared by both the streaming and non-streaming paths.
type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	System      string    `json:"system,omitempty"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature float64   `json:"temperature,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
}

// chatResponse is Anthropic's wire representation of a completed
// (non-streaming) messages response.
type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// errorResponse is Anthropic's wire representation of an error body.
type errorResponse struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// splitMessages separates gateway RoleSystem messages (concatenated,
// in order, into a single system prompt) from the rest of the
// conversation, matching Anthropic's request shape.
func splitMessages(msgs []provider.Message) (system string, out []message) {
	var systemParts []string
	for _, m := range msgs {
		if m.Role == provider.RoleSystem {
			systemParts = append(systemParts, m.Content)
			continue
		}
		out = append(out, message{Role: string(m.Role), Content: m.Content})
	}
	return strings.Join(systemParts, "\n"), out
}

// ChatCompletion implements provider.Provider.
func (p *Provider) ChatCompletion(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	system, msgs := splitMessages(req.Messages)
	body := chatRequest{
		Model:       req.Model,
		Messages:    msgs,
		System:      system,
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
		return nil, fmt.Errorf("anthropic: decode response: %w", err)
	}

	var text strings.Builder
	for _, c := range out.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}

	return &provider.ChatResponse{
		ID:    out.ID,
		Model: out.Model,
		Choices: []provider.Choice{
			{
				Index:        0,
				Message:      provider.Message{Role: provider.RoleAssistant, Content: text.String()},
				FinishReason: out.StopReason,
			},
		},
		Usage: provider.Usage{
			PromptTokens:     out.Usage.InputTokens,
			CompletionTokens: out.Usage.OutputTokens,
			TotalTokens:      out.Usage.InputTokens + out.Usage.OutputTokens,
		},
	}, nil
}

// ChatCompletionStream implements provider.Provider. It performs the
// upstream request, validates the initial status code, and — once the
// response is confirmed successful — returns a StreamReader that
// parses the SSE body incrementally as Recv is called.
func (p *Provider) ChatCompletionStream(ctx context.Context, req *provider.ChatRequest) (provider.StreamReader, error) {
	system, msgs := splitMessages(req.Messages)
	body := chatRequest{
		Model:       req.Model,
		Messages:    msgs,
		System:      system,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stream:      true,
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
		return nil, fmt.Errorf("anthropic: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", apiVersion)

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

// streamReader implements provider.StreamReader over an Anthropic SSE
// response body. It reads line by line via bufio.Scanner rather than
// buffering the whole body, so chunks are yielded to the caller as
// they arrive.
//
// Anthropic's SSE events are named ("event: {type}") with the payload
// on the following "data: {json}" line, so Recv tracks the most
// recently seen event name and interprets each data line accordingly.
type streamReader struct {
	ctx     context.Context
	body    io.ReadCloser
	scanner *bufio.Scanner

	// err is the terminal error (or io.EOF) once the stream has ended;
	// once set, every subsequent Recv returns it unchanged.
	err error

	closeOnce sync.Once
	closeErr  error

	// promptTokens is captured from message_start and re-emitted
	// alongside output tokens once message_delta reports them, since
	// Anthropic splits input/output usage across two separate events.
	promptTokens int
}

// Recv implements provider.StreamReader.
func (r *streamReader) Recv() (*provider.StreamChunk, error) {
	if r.err != nil {
		return nil, r.err
	}

	var event string
	for r.scanner.Scan() {
		line := r.scanner.Text()

		if ev, ok := strings.CutPrefix(line, "event: "); ok {
			event = ev
			continue
		}

		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}

		switch event {
		case "message_start":
			var msg struct {
				Message struct {
					Usage struct {
						InputTokens int `json:"input_tokens"`
					} `json:"usage"`
				} `json:"message"`
			}
			if err := json.Unmarshal([]byte(data), &msg); err != nil {
				r.err = fmt.Errorf("anthropic: decode message_start: %w", err)
				return nil, r.err
			}
			r.promptTokens = msg.Message.Usage.InputTokens

		case "content_block_delta":
			var d struct {
				Delta struct {
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal([]byte(data), &d); err != nil {
				r.err = fmt.Errorf("anthropic: decode content_block_delta: %w", err)
				return nil, r.err
			}
			return &provider.StreamChunk{Delta: d.Delta.Text}, nil

		case "message_delta":
			var d struct {
				Delta struct {
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
				Usage struct {
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal([]byte(data), &d); err != nil {
				r.err = fmt.Errorf("anthropic: decode message_delta: %w", err)
				return nil, r.err
			}
			return &provider.StreamChunk{
				FinishReason: d.Delta.StopReason,
				Usage: &provider.Usage{
					PromptTokens:     r.promptTokens,
					CompletionTokens: d.Usage.OutputTokens,
					TotalTokens:      r.promptTokens + d.Usage.OutputTokens,
				},
			}, nil

		case "message_stop":
			r.err = io.EOF
			return nil, io.EOF

		case "error":
			var e errorResponse
			_ = json.Unmarshal([]byte(data), &e)
			r.err = fmt.Errorf("anthropic: stream error: %s", e.Error.Message)
			return nil, r.err

		default:
			// ping, content_block_start, content_block_stop, and any
			// other event types carry nothing the gateway normalizes.
		}
	}

	// The scan loop ended without a message_stop event: either the
	// read failed outright (scanner.Err), or the connection was torn
	// down as a side effect of context cancellation, which some
	// transports surface as a clean io.EOF rather than an explicit
	// error — so ctx.Err is checked explicitly before falling back to
	// a normal end-of-stream io.EOF.
	if err := r.scanner.Err(); err != nil {
		r.err = fmt.Errorf("anthropic: read stream: %w", err)
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
