SHELL := /bin/bash

SWAG        := $(shell command -v swag 2>/dev/null || echo $(HOME)/go/bin/swag)
SWAG_FLAGS  := init -g api.go -d . --ot json -o ./docs --parseDependency -q
OPENAPI_DIR := internal/api/docs

.PHONY: build test openapi clean

build:
	go build ./...

test:
	go test ./...

# openapi regenerates the committed OpenAPI 2.0 document
# (internal/api/docs/swagger.json) from the swag annotations in
# internal/api/api.go. The goose-sdk-ts generator consumes this file.
openapi:
	cd internal/api && $(SWAG) $(SWAG_FLAGS)
	rm -f $(OPENAPI_DIR)/docs.go
