FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o /out/e2e-cancel .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/e2e-cancel /usr/local/bin/e2e-cancel
ENTRYPOINT ["/usr/local/bin/e2e-cancel"]