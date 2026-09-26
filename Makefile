.PHONY: help build run test test-coverage load-test clean deps fmt vet

help:
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@echo '  build          Build the application binary'
	@echo '  run            Run server locally'
	@echo '  test           Run all tests'
	@echo '  test-coverage  Run tests with coverage analysis'
	@echo '  load-test      Run heavy load & stress testing benchmark'
	@echo '  clean          Clean test artifacts'
	@echo '  deps           Download dependencies and tidy'
	@echo '  fmt            Format code with go fmt and gofmt'
	@echo '  vet            Run go vet'

build:
	go build -o bin/broker-service ./cmd/server

run:
	go run ./cmd/server

test:
	go test -v ./...

test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

load-test:
	go run ./tests/load -duration 10s -rps 5000 -workers 500

clean:
	rm -f bin/broker-service coverage.out

deps:
	go mod download
	go mod tidy

fmt:
	go fmt ./...
	gofmt -s -w .

vet:
	go vet ./...

.DEFAULT_GOAL := help
