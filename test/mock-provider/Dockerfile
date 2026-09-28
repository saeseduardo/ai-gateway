FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod main.go .
RUN CGO_ENABLED=0 go build -o /out/mock-provider .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mock-provider /usr/local/bin/mock-provider
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/mock-provider"]
