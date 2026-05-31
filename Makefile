BINARY     := bin/erminetq
CMD        := ./cmd/server
MODULE     := github.com/peifengstudio/erminetq

# Read DB path from env, fall back to local default
DB         ?= $(ERMINETQ_DB)
ifeq ($(DB),)
DB         := erminetq.db
endif

# Build info injected at link time
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT     := $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS    := -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

.DEFAULT_GOAL := help

# ── Setup ─────────────────────────────────────────────────────────────────────

.PHONY: deps
deps: ## Download all Go module dependencies into the local cache
	go mod download
	go mod verify
	@echo "Dependencies ready"

# ── Build ──────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build the binary to bin/erminetq
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) $(CMD)
	@echo "Built $(BINARY) ($(VERSION))"

.PHONY: build-release
build-release: ## Build a stripped release binary
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS) -s -w" -o $(BINARY) $(CMD)

# ── Run ────────────────────────────────────────────────────────────────────────

.PHONY: dev
dev: ## Start dev server with hot-reload via air (install: go install github.com/air-verse/air@latest)
	@which air > /dev/null 2>&1 || { echo "air not found — run: go install github.com/air-verse/air@latest"; exit 1; }
	air

.PHONY: run
run: ## Run the server once with go run (no hot-reload); respects ERMINETQ_DB
	go run $(CMD) server -db $(DB)

.PHONY: migrate
migrate: ## Apply pending migrations and exit; respects ERMINETQ_DB
	go run $(CMD) migrate -db $(DB)

# ── Test ───────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all tests
	go test ./...

.PHONY: test-v
test-v: ## Run all tests with verbose output
	go test -v ./...

.PHONY: test-cover
test-cover: ## Run tests and open HTML coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: test-store
test-store: ## Run only the store package tests
	go test -v -count=1 ./internal/store/...

# ── Quality ────────────────────────────────────────────────────────────────────

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: fmt
fmt: ## Format all Go source files
	gofmt -w .

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy

# ── Examples ──────────────────────────────────────────────────────────────────

# Examples — all target the dev server started by `make dev` (:8080).
#
# Go-only example:
#   Terminal 1: make dev
#   Terminal 2: make example-submit
#
# Python SDK example:
#   Terminal 1: make dev
#   Terminal 2: make example-py-worker
#   Terminal 3: make example-py-submit

EXAMPLE_SERVER ?= http://localhost:8080

.PHONY: example-submit
example-submit: ## Submit Go example tasks to the dev server (make dev must be running)
	@curl -sf $(EXAMPLE_SERVER)/api/workers > /dev/null 2>&1 || \
	  { echo "✗ server not running — start it first: make dev"; exit 1; }
	go run ./examples/go/submit -addr $(EXAMPLE_SERVER)

.PHONY: example-py-worker
example-py-worker: ## Start the Python SDK worker (make dev must be running)
	@curl -sf $(EXAMPLE_SERVER)/api/workers > /dev/null 2>&1 || \
	  { echo "✗ server not running — start it first: make dev"; exit 1; }
	cd examples/python && ERMINETQ_URL=$(EXAMPLE_SERVER) uv run python worker.py

.PHONY: example-py-submit
example-py-submit: ## Submit Python SDK tasks to the dev server
	@curl -sf $(EXAMPLE_SERVER)/api/workers > /dev/null 2>&1 || \
	  { echo "✗ server not running — start it first: make dev"; exit 1; }
	cd examples/python && uv run python submit.py --addr $(EXAMPLE_SERVER)

# ── Clean ──────────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build artefacts and local database
	rm -f $(BINARY)
	rm -f coverage.out
	@echo "Cleaned"

.PHONY: clean-db
clean-db: ## Remove the local development database (ERMINETQ_DB or erminetq.db)
	rm -f $(DB) $(DB)-wal $(DB)-shm
	@echo "Removed $(DB)"

# ── Help ───────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
