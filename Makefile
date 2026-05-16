GO ?= go
PKGS ?= ./...

.PHONY: help fmt fmt-check test vet tidy check ci clean

help:
	@printf '%s\n' \
		'Targets:' \
		'  fmt    Format Go files' \
		'  fmt-check  Verify Go files are formatted' \
		'  test   Run tests' \
		'  vet    Run go vet' \
		'  tidy   Update go.mod and go.sum' \
		'  check  Run fmt, vet, and test' \
		'  ci     Run fmt-check, vet, and test' \
		'  clean  Clear Go build and test caches'

fmt:
	$(GO)fmt -w .

fmt-check:
	@test -z "$$($(GO)fmt -l .)" || { $(GO)fmt -l .; exit 1; }

test:
	$(GO) test $(PKGS)

vet:
	$(GO) vet $(PKGS)

tidy:
	$(GO) mod tidy

check: fmt vet test

ci: fmt-check vet test

clean:
	$(GO) clean -cache -testcache
