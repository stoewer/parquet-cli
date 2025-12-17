BINARY     ?= parquet-cli

GO_BUILD_OPTS  := -v -mod=readonly
GO_TEST_OPTS   := -v -mod=readonly -race -count=1 -cover -coverprofile=profile.cov
GO_LINT_OPTS   := --config ./golangci.yml

VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null)
GO_LDFLAGS := -X main.Version=$(VERSION) -X main.Commit=$(COMMIT)

.PHONY: build
build:
	go build $(GO_BUILD_OPTS) -ldflags '$(GO_LDFLAGS)' -o ./${BINARY} ./cmd/${BINARY}

.PHONY: test
test:
	go test $(GO_TEST_OPTS) ./...

.PHONY: lint
lint:
	golangci-lint run $(GO_LINT_OPTS) ./...

.PHONY: clean
clean:
	rm -vf ./${BINARY} ./profile.cov
	go clean -cache -testcache
