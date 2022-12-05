BINARY     ?= parquet-cli

GO_BUILD_OPTS  := -v -mod=readonly
GO_TEST_OPTS   := -v -mod=readonly -race -count=1 -cover -coverprofile=profile.cov
GO_LINT_OPTS   := --config ./golangci.yml

.PHONY: build
build:
	go build $(GO_BUILD_OPTS) -o ./${BINARY} ./cmd/${BINARY}

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
