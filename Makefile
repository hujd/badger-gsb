#
# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
#

USER_ID      := $(shell id -u)
HAS_JEMALLOC := $(shell test -f /usr/local/lib/libjemalloc.a && echo jemalloc)
JEMALLOC_VER := 5.3.1
JEMALLOC_URL := https://github.com/jemalloc/jemalloc/releases/download/$(JEMALLOC_VER)/jemalloc-$(JEMALLOC_VER).tar.bz2

.PHONY: all badger test jemalloc dependency

badger: jemalloc
	@echo "Compiling Badger binary..."
	@$(MAKE) -C badger badger
	@echo "Badger binary located in badger directory."

test: jemalloc
	@echo "Running Badger tests..."
	@./test.sh

jemalloc:
	@if [ -z "$(HAS_JEMALLOC)" ]; then \
		mkdir -p /tmp/jemalloc-temp && cd /tmp/jemalloc-temp; \
		echo "Downloading jemalloc..."; \
		curl -fsSL "$(JEMALLOC_URL)" -o jemalloc.tar.bz2; \
		tar xjf jemalloc.tar.bz2; \
		cd jemalloc-$(JEMALLOC_VER); \
		./configure --with-jemalloc-prefix=je_ --with-malloc-conf=background_thread:true,metadata_thp:auto; \
		$(MAKE); \
		if [ "$(USER_ID)" -eq 0 ]; then \
			$(MAKE) install; \
		else \
			echo "==== Need sudo access to install jemalloc"; \
			sudo $(MAKE) install; \
		fi; \
	fi

dependency:
	@echo "Installing dependencies..."
	@sudo apt-get update
	@sudo apt-get -y install \
		ca-certificates \
		curl \
		gnupg \
		lsb-release \
		build-essential \
		protobuf-compiler

# ---------------------------------------------------------------------------
# Local, fully-offline CI targets.
#
# These targets never touch the network: they build in vendor mode with the
# module proxy disabled and the local Go toolchain pinned. They do NOT use
# the jemalloc build tag (ristretto falls back to its pure-Go allocator), so
# they work on machines without /usr/local/lib/libjemalloc.a. The CI-only
# `make test` target above is unchanged and still uses jemalloc.
# ---------------------------------------------------------------------------

# Offline environment for every go invocation in the targets below.
GO_OFFLINE := GOFLAGS=-mod=vendor GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local

# Coverage gate threshold in percent. Override e.g. COVERAGE_MIN=99 make coverage-gate
COVERAGE_MIN  ?= 55
COVERAGE_FILE ?= coverage.out
DIST_DIR      ?= dist

# Quick root-package smoke tests for ci-fast (the full root suite takes
# minutes; these finish in about a second).
SMOKE_TESTS := TestTxnSimple$$|TestUpdateAndView$$|TestGet$$|TestConcurrentWrite$$|TestManagedDB$$

.PHONY: ci ci-fast build-local test-cover coverage-gate cross-compile

ci: build-local test-cover coverage-gate cross-compile
	@echo "==> make ci: all checks passed"

# Fast development loop: compile everything, run the quick subpackage tests
# plus a small root-package smoke set. Seconds, not minutes.
ci-fast:
	@echo "==> Building all packages (offline, no jemalloc)"
	$(GO_OFFLINE) go build ./...
	@echo "==> Running fast unit tests (offline)"
	$(GO_OFFLINE) go test -count=1 ./y/... ./skl/... ./table/... ./trie/... ./badger/...
	$(GO_OFFLINE) go test -count=1 -run '$(SMOKE_TESTS)' .
	@echo "==> make ci-fast: OK"

build-local:
	@echo "==> Building all packages (offline, no jemalloc)"
	$(GO_OFFLINE) go build ./...

# Single pass over the whole module: runs the tests, the race detector and
# coverage collection together.
test-cover:
	@echo "==> Running full test suite with -race and coverage (offline)"
	$(GO_OFFLINE) go test -race -count=1 -timeout=30m -coverprofile=$(COVERAGE_FILE) ./...

# Standalone coverage gate. Requires $(COVERAGE_FILE) (produced by
# 'make test-cover' or 'make ci'). Fails if total coverage < COVERAGE_MIN.
coverage-gate:
	@test -f $(COVERAGE_FILE) || { \
		echo "error: $(COVERAGE_FILE) not found. Run 'make test-cover' (or 'make ci') first."; \
		exit 1; \
	}
	@total=$$($(GO_OFFLINE) go tool cover -func=$(COVERAGE_FILE) | \
		awk '/^total:/ { gsub(/%/, "", $$NF); print $$NF }'); \
	echo "==> Total coverage: $${total}% (gate: $(COVERAGE_MIN)%)"; \
	if awk -v total="$$total" -v min="$(COVERAGE_MIN)" 'BEGIN { exit (total + 0 >= min + 0) ? 0 : 1 }'; then \
		echo "==> coverage gate PASSED"; \
	else \
		echo "==> coverage gate FAILED: $${total}% < $(COVERAGE_MIN)%"; \
		exit 1; \
	fi

# Cross-compile the badger CLI for the two shipping platforms.
cross-compile:
	@echo "==> Cross-compiling badger CLI into $(DIST_DIR)/ (offline, no jemalloc)"
	@mkdir -p $(DIST_DIR)
	$(GO_OFFLINE) CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o $(DIST_DIR)/badger-darwin-arm64 ./badger
	$(GO_OFFLINE) CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build -o $(DIST_DIR)/badger-linux-amd64 ./badger
	@echo "==> Cross-compile artifacts:"
	@ls -lh $(DIST_DIR)
