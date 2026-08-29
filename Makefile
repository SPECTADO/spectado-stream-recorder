VERSION ?= $(shell tr -d '[:space:]' < VERSION)
IMAGE   ?= stream-recorder:dev

.PHONY: build test vet fmt docker compose-up compose-down e2e clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/recorder ./cmd/recorder

test:
	go test ./... -race -count=1

vet:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

fmt:
	gofmt -w ./cmd ./internal

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

compose-up:
	@test -f .env || cp .env.example .env
	docker compose -f docker-compose.example.yml up --build

compose-down:
	docker compose -f docker-compose.example.yml down -v

# Local smoke test with the real ffmpeg (no Docker): records a generated HLS
# tone for ~50 s, suspends/resumes via SIGTERM, validates the file with ffprobe.
e2e:
	./scripts/e2e-local.sh

clean:
	rm -rf bin
