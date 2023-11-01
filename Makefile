.PHONY: test lint
test:
	go test ./... --test.v -cover -coverprofile=coverage.out; go tool cover -html=coverage.out
lint:
	golangci-lint run
