VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test bundle image clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/repeatertastic-meshflow ./cmd/repeatertastic-meshflow

test:
	go vet ./...
	go test -race ./...

# The zip to install in RepeaterTastic (Plugins → Install plugin).
bundle:
	./scripts/bundle.sh $(VERSION)

# Image for running the plugin attached (next to RepeaterTastic, not inside it).
image:
	docker build --build-arg VERSION=$(VERSION) -t repeatertastic-meshflow .

clean:
	rm -rf bin dist
