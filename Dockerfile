# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /out/gateway /usr/local/bin/gateway

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/gateway"]
