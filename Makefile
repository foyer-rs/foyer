SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all monitor clear

deps:
	./scripts/install-deps.sh

check:
	shellcheck ./scripts/*
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets --features deadlock
	cargo clippy --all-targets --features tokio-console
	cargo clippy --all-targets --features trace
	cargo clippy --all-targets
	cargo udeps --workspace --exclude foyer-workspace-hack

test:
	RUST_BACKTRACE=1 cargo nextest run --all
	RUST_BACKTRACE=1 cargo test --doc

test-ignored:
	RUST_BACKTRACE=1 cargo test --package foyer-common -- --nocapture --ignored

test-all: test test-ignored

all: check test-all

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp
