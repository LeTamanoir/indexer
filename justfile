_default:
    @just --list

fmt:
    go fmt .

fmt-check:
    @files="$(gofmt -l .)"; test -z "$files" || { printf '%s\n' "$files"; exit 1; }

test:
    go test ./...

vet:
    go vet ./...

tidy:
    go mod tidy

check: fmt vet test

ci: fmt-check vet test

clean:
    go clean -cache -testcache
