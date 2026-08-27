.PHONY: build test test-race run tidy lint docker-build docker-run

GO_TEST_IMAGE := golang:1.23

build:
	go build -o bin/gateway ./cmd/gateway

test:
	go test ./...

# -race needs cgo + a C toolchain, which the alpine-based build image
# doesn't carry; run it in a throwaway Debian-based golang container
# instead, with the module cache kept in a named volume for speed.
test-race:
	MSYS_NO_PATHCONV=1 docker run --rm -e CGO_ENABLED=1 \
		-v "$(CURDIR)":/src -w /src \
		-v ai-gateway-gomod-cache:/go/pkg/mod \
		$(GO_TEST_IMAGE) go test ./... -race

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
