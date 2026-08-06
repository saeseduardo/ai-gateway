.PHONY: build test run tidy lint docker-build docker-run

build:
	go build -o bin/gateway ./cmd/gateway

test:
	go test ./...

run:
	go run ./cmd/gateway

tidy:
	go mod tidy

lint:
	go vet ./...

docker-build:
	docker build -t ai-gateway .

docker-run: docker-build
	docker run --rm ai-gateway
