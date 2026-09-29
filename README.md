# AI Gateway

A Go proxy that puts OpenAI and Anthropic behind one OpenAI-compatible API, with streaming that actually stops paying for tokens when the client hangs up.

🚧 **Active work in progress.** Config, both providers, end-to-end streaming (including cancellation and mid-stream errors), and the resilience utilities (retry with exponential backoff and a circuit breaker, tested under `-race`) are done. The resilience utilities are not wired into request handling yet; rate limiting and observability are not built — see [Roadmap](#roadmap).

## Why

Every LLM provider speaks its own dialect: different request shapes, different SSE event framing, different error codes for the same failure. A gateway's job is to absorb that variance behind one contract so the rest of a system — routing, rate limiting, billing, observability — is written once, against providers, not against OpenAI-with-an-if-Anthropic-then. The obvious part of that job is request/response translation. The part that's actually interesting, and where most of the engineering effort in this repo went, is streaming: an SSE connection is long-lived, stateful, and — because tokens cost money — has a cost profile that keeps changing for as long as it's open. Getting the request/response mapping right is table stakes; getting cancellation right (so a client that disconnects mid-generation stops the upstream request within milliseconds, not when some timeout eventually fires) and getting mid-stream failure right (so an upstream error after the response has already started doesn't just hang or silently truncate) is where a toy proxy turns into something you'd trust in production.

## Architecture

![Architecture](docs/architecture.png)

The gateway is built around one idea: a normalized internal contract (`provider.ChatRequest` / `ChatResponse` / `StreamChunk`) that every upstream implementation translates into and out of, so nothing outside the `provider` package's implementations (`openai`, `anthropic`) ever sees a provider-specific shape. HTTP request handling (`internal/server`) and the public wire format (`internal/api`) are kept as separate layers from that internal contract, so the public API can stay OpenAI-compatible even as internal provider handling evolves. A `Router` maps a requested model name to the `provider.Provider` responsible for it, so adding a provider is a matter of implementing that interface, not touching the HTTP layer. Cancellation is handled by threading the inbound request's `context.Context` all the way through to the upstream provider call, rather than as a separate mechanism bolted on top.

## What works today

- **Configuration** (`internal/config`): fail-fast loading from environment variables, required `OPENAI_API_KEY` / `ANTHROPIC_API_KEY`, sane defaults for everything else, malformed values collected and reported together rather than one at a time.
- **Two providers** (`internal/openai`, `internal/anthropic`), each implementing `ChatCompletion` (non-streaming) and `ChatCompletionStream` (SSE) against the real upstream HTTP APIs, hand-rolled — no vendor SDK.
- **`POST /v1/chat/completions`**, OpenAI-compatible request/response shape, both modes:
  - Non-streaming: full JSON round trip.
  - Streaming (SSE): headers flushed immediately (before the first chunk), every frame flushed individually, `X-Accel-Buffering: no` so reverse proxies don't buffer it, and a configurable keep-alive heartbeat comment while waiting on a slow upstream.
  - **Client-side cancellation, end-to-end**: closing the client connection cancels the request context, which is watched explicitly in the streaming loop and is also bound to the upstream provider's HTTP request — the upstream connection is cut immediately instead of the gateway continuing to consume (and pay for) tokens nobody will read. Covered by a dedicated test run repeatedly under `-race`.
  - **Mid-stream error handling**: an upstream failure that happens after the response has already started (status 200 and some frames already sent) is reported as an SSE error frame followed by `[DONE]`, since the HTTP status can no longer change at that point. Clean completion, client cancellation, and mid-stream failure are logged as three distinct events (`stream_completed`, `stream_cancelled_by_client`, `stream_failed`).
- **Health checks**: `GET /healthz` (liveness) and `GET /readyz` (readiness — currently a stub that always returns ready; see [Roadmap](#roadmap)).
- **Resilience module** (`internal/resilience`): retry with exponential backoff (capped delay, optional jitter, context-aware — a cancelled context aborts a waiting retry immediately) and a concurrency-safe circuit breaker (Closed/Open/HalfOpen, typed `ErrCircuitOpen`, injectable clock for tests). Built as an isolated, tested package; not yet connected to provider calls.
- **Structured JSON logging** (`log/slog`) with per-request IDs, panic recovery middleware, and graceful shutdown on `SIGINT`/`SIGTERM`.
- **Test suite** across every package, run under `go test ./... -race` with no data races.

## Design decisions

- **A normalized internal contract instead of adapting to one provider's shape.** `provider.ChatRequest`/`ChatResponse`/`StreamChunk` exist so the HTTP layer, the router, and every test outside `internal/openai`/`internal/anthropic` are written once, against an interface, not against OpenAI's JSON with Anthropic bolted on. Adding a third provider means implementing `provider.Provider`; nothing else changes.
- **Hand-rolled HTTP clients instead of the official SDKs.** Full control over exactly how context cancellation propagates into an in-flight streaming read, exactly what "timeout" means for a streaming vs. non-streaming call, and exactly how each provider's SSE framing maps onto `provider.StreamReader` — without inheriting a vendor SDK's own retry/timeout opinions, which would otherwise fight with the gateway's.
- **Separate HTTP clients per mode, not one client with one timeout.** Both providers build a `Timeout`-bounded client for non-streaming round trips and a second, timeout-free client for streaming, whose lifecycle is governed entirely by the request's context instead. A single client can't correctly express both "a request that hangs should fail fast" and "a streaming response may legitimately stay open far longer than a typical request."
- **Context propagation is the mechanism for the gateway's central cost property, not an afterthought.** Since streaming tokens are billed per token generated, `r.Context()` is passed into the provider call and is *also* watched explicitly in the streaming select loop — deliberately redundant with the provider-level propagation, so cancellation doesn't depend on every current and future provider implementation wiring up context correctly on its own.
- **The SSE error frame exists because of a protocol constraint, not a design preference.** Once the 200 status and the first `data:` frame are flushed to the client, the HTTP status can never change — there's no way to turn a streaming response into a 500 partway through. So a mid-stream upstream failure is reported as one final `data:` frame carrying the same `{"error":{"message":...,"type":...}}` shape the non-streaming error path uses, followed by `data: [DONE]\n\n`, so the client can tell "the model finished" from "the stream broke" without a status code.
- **Fault tolerance first as an isolated, tested package.** The retry/circuit-breaker machinery lives in `internal/resilience` with no dependency on the HTTP layer; only `DefaultIsRetryable` knows the provider error type, via `errors.As`. It is deliberately not wired into the request path yet — the mechanics were proven in isolation under `-race` before integration, so wiring them in later is a contained change.

## Getting started

### Prerequisites

- Go 1.23 (see `go.mod`)
- Docker, for `docker compose` and for running `-race` tests in a container that has a C toolchain (see below)

### Environment variables

Only the two API keys are required; everything else has a default.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `OPENAI_API_KEY` | **yes** | — | |
| `ANTHROPIC_API_KEY` | **yes** | — | |
| `GATEWAY_OPENAI_BASE_URL` | no | `https://api.openai.com/v1` | |
| `GATEWAY_OPENAI_TIMEOUT` | no | `30s` | non-streaming requests only |
| `GATEWAY_ANTHROPIC_BASE_URL` | no | `https://api.anthropic.com` | |
| `GATEWAY_ANTHROPIC_TIMEOUT` | no | `30s` | non-streaming requests only |
| `GATEWAY_SERVER_PORT` | no | `8080` | |
| `GATEWAY_SERVER_READ_TIMEOUT` | no | `15s` | |
| `GATEWAY_SERVER_WRITE_TIMEOUT` | no | `15s` | see note below |
| `GATEWAY_SERVER_IDLE_TIMEOUT` | no | `60s` | |
| `GATEWAY_SERVER_SHUTDOWN_TIMEOUT` | no | `10s` | |
| `GATEWAY_REDIS_ADDRESS` | no | `localhost:6379` | parsed, not yet used by any code path |
| `GATEWAY_REDIS_PASSWORD` | no | `""` | parsed, not yet used |
| `GATEWAY_REDIS_DB` | no | `0` | parsed, not yet used |
| `GATEWAY_TELEMETRY_METRICS_PORT` | no | `9090` | parsed, no metrics listener wired up yet |

> **Note on `GATEWAY_SERVER_WRITE_TIMEOUT`:** `net/http.Server.WriteTimeout` starts counting when a request's headers are read and is not reset per write, including for a streaming response. A stream that stays open longer than this value can be cut by the server itself regardless of the SSE keep-alive heartbeat. The current default (15s) has not been raised to account for this; if you expect completions that stream longer than that, raise it explicitly until this is addressed properly (see [Roadmap](#roadmap)).

### Run it

With Docker Compose (gateway + a Redis instance reserved for future use):

```bash
cp .env.example .env
# edit .env and fill in OPENAI_API_KEY / ANTHROPIC_API_KEY
docker compose up -d --build
curl http://localhost:8080/healthz
```

Or natively, via the Makefile:

```bash
export OPENAI_API_KEY=sk-...
export ANTHROPIC_API_KEY=sk-ant-...
make run          # go run ./cmd/gateway
# or
make build && ./bin/gateway
```

### Run the tests

```bash
make test         # go test ./...
make test-race     # go test ./... -race, in a golang:1.23 (Debian) container --
                    # the alpine-based Dockerfile build image has no C toolchain,
                    # and -race needs cgo
```

### Try it

Non-streaming:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Say hello in one word."}]
  }'
```

Streaming (`--no-buffer` so curl prints each SSE frame as it arrives instead of waiting for the response to finish):

```bash
curl --no-buffer http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-5",
    "messages": [{"role": "user", "content": "Count from 1 to 5."}],
    "stream": true
  }'
```

Models currently routed (hardcoded in `cmd/gateway/main.go`, see [Roadmap](#roadmap)): `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`, `gpt-3.5-turbo` → OpenAI; `claude-sonnet-5`, `claude-opus-5`, `claude-haiku-4-5-20251001` → Anthropic.

## Roadmap

**Done:**
- [x] Fail-fast environment-based configuration
- [x] Normalized `Provider` contract, implemented for OpenAI and Anthropic (non-streaming + streaming)
- [x] `/v1/chat/completions`: non-streaming and SSE streaming, OpenAI-compatible wire format
- [x] SSE hardening: immediate header flush, per-frame flush, `X-Accel-Buffering`, configurable keep-alive heartbeat
- [x] End-to-end client cancellation, cutting the upstream connection immediately (race-tested)
- [x] Mid-stream error handling (SSE error frame + `[DONE]`, distinguishable log events)
- [x] Structured logging, panic recovery, graceful shutdown
- [x] `docker-compose.yml` for local development
- [x] Resilience utilities: retry with exponential backoff and circuit breaker (isolated, race-tested)

**Not done yet:**
- [ ] Config-driven model→provider routing (currently a hardcoded list in `main.go`)
- [ ] Rate limiting (Redis config already scaffolded; no client or middleware wired up)
- [ ] Wire retry + circuit breaker into provider calls (both exist, isolated and tested), then provider failover
- [ ] Observability: Prometheus metrics, Grafana dashboards (metrics port reserved, no listener yet)
- [ ] `/readyz` actually checking Redis/provider connectivity instead of always returning ready
- [ ] Fix `WriteTimeout` for long-lived streaming responses (see note above)

## Tech stack

- **Go 1.23**
- [`github.com/go-chi/chi/v5`](https://github.com/go-chi/chi) v5.3.1 — routing and middleware; the only non-stdlib dependency
- Standard library for everything else: `net/http`, `encoding/json`, `log/slog`, `context`
- Docker + Docker Compose for local development
