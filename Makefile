PREFIX  ?= $(HOME)/.local
BINDIR  ?= $(PREFIX)/bin
VERSION ?= dev
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -s -w

# 交叉编译：make build GOOS=linux GOARCH=amd64
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all build build-cli build-daemon install install-cli install-daemon test vet fmt fmt-check clean

all: build

build: build-cli build-daemon

build-cli:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags="$(LDFLAGS)" -o bin/pantheon ./cmd/pantheon

build-daemon:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags="$(LDFLAGS)" -o bin/pantheond ./cmd/pantheond

install: install-cli install-daemon

install-cli: build-cli
	install -d $(BINDIR)
	install -m 0755 bin/pantheon $(BINDIR)/pantheon

install-daemon: build-daemon
	install -d $(BINDIR)
	install -m 0755 bin/pantheond $(BINDIR)/pantheond

test:
	go test -count=1 -timeout 120s ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
	goimports -w . 2>/dev/null || true

fmt-check:
	@diff=$$(gofmt -l .); if [ -n "$$diff" ]; then \
		echo "gofmt found unformatted files:"; echo "$$diff"; exit 1; fi

clean:
	rm -rf bin/
