#
# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
#

USER_ID      := $(shell id -u)
HAS_JEMALLOC := $(shell test -f /usr/local/lib/libjemalloc.a && echo jemalloc)
JEMALLOC_VER := 5.3.1
JEMALLOC_URL := https://github.com/jemalloc/jemalloc/releases/download/$(JEMALLOC_VER)/jemalloc-$(JEMALLOC_VER).tar.bz2

# Offline, vendored Go invocations used by the local CI targets below.
# GOPROXY=off guarantees nothing is fetched from the network.
GO_OFFLINE   := GOFLAGS=-mod=vendor GOPROXY=off

# Minimum total statement coverage enforced by `make coverage-gate`.
# Override ad hoc, e.g. `COVERAGE_MIN=99 make coverage-gate`.
COVERAGE_MIN ?= 55

# Fast smoke subset of the root package tests used by `make ci-fast`.
CI_FAST_ROOT_TESTS := ^(TestGet|TestWrite|TestUpdateAndView|TestGetAfterDelete|TestTxnTooBig|TestSequence|TestIterate2Basic|TestInvalidKey|TestIsClosed|TestMaxVersion|TestConcurrentWrite|TestGetSetRace)$$

.PHONY: all badger test jemalloc dependency ci ci-fast ci-build ci-test coverage-gate cross-compile

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
# Local CI. Fully offline: builds with -mod=vendor and GOPROXY=off, never
# downloads jemalloc or anything else. `make test` above is unchanged and
# remains what CI runs.
# ---------------------------------------------------------------------------

ci: ci-build ci-test coverage-gate cross-compile
	@echo "==> Local CI passed (coverage >= $(COVERAGE_MIN)%)"

ci-fast:
	@echo "==> Building all packages (offline)"
	$(GO_OFFLINE) go build ./...
	@echo "==> Running subpackage tests"
	$(GO_OFFLINE) go test -count=1 -timeout=5m ./skl/... ./table/... ./y/... ./trie/... ./badger/...
	@echo "==> Running root package smoke tests"
	$(GO_OFFLINE) go test -count=1 -timeout=5m -run '$(CI_FAST_ROOT_TESTS)' .
	@echo "==> ci-fast passed"

ci-build:
	@echo "==> Building all packages (offline)"
	$(GO_OFFLINE) go build ./...
	@echo "==> Building badger CLI"
	$(GO_OFFLINE) go build -o badger/badger ./badger

ci-test:
	@echo "==> Running full test suite with race detector and coverage"
	$(GO_OFFLINE) go test -race -count=1 -timeout=30m \
		-covermode=atomic -coverprofile=cover.out ./...

coverage-gate:
	@test -f cover.out || { \
		echo "cover.out not found. Run 'make ci-test' (or 'make ci') first."; \
		exit 1; \
	}
	@total=$$(go tool cover -func=cover.out | awk '/^total:/ { gsub(/%/, "", $$NF); print $$NF }'); \
	test -n "$$total" || { echo "could not parse total coverage from cover.out"; exit 1; }; \
	echo "==> Total coverage: $${total}% (minimum: $(COVERAGE_MIN)%)"; \
	if awk -v total="$$total" -v min="$(COVERAGE_MIN)" 'BEGIN { exit !(total + 0 < min + 0) }'; then \
		echo "==> coverage gate FAILED: $${total}% is below $(COVERAGE_MIN)%"; \
		exit 1; \
	fi

cross-compile:
	@echo "==> Cross-compiling badger CLI into dist/"
	@mkdir -p dist
	$(GO_OFFLINE) CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o dist/badger-darwin-arm64 ./badger
	$(GO_OFFLINE) CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build -o dist/badger-linux-amd64 ./badger
	@echo "==> Wrote dist/badger-darwin-arm64 and dist/badger-linux-amd64"
