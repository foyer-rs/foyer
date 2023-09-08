SHELL := /bin/bash
.PHONY: proto check test deps monitor

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

test:
	RUST_BACKTRACE=1 cargo nextest run --all
	RUST_BACKTRACE=1 cargo test --doc
	
monitor:
	./scripts/monitor.sh

clear:
	cargo clean
	rm -rf .tmp