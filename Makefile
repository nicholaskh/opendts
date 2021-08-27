ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BIN_OUT:=$(ROOT)/bin/dts
PKG:=$(shell go list -m)

.PHONY: all build dts clean test build_with_coverage
all: build #test

build: dts

dts:
	go build -o $(BIN_OUT) cmd/dts/main.go

clean:
	@rm -rf bin
	@rm -f .coverage.out .coverage.html

test:
	go test -coverprofile=.coverage.out ./...
	go tool cover -func=.coverage.out -o .coverage.func
	tail -1 .coverage.func
	go tool cover -html=.coverage.out -o .coverage.html

build_with_coverage:
	go test -c cmd/dts/main.go -coverpkg ./... -covermode=count -o bin/dts
