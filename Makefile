SHELL := /bin/bash
.PHONY: proto check test deps

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
	RUST_BACKTRACE=1 cargo test --all

jaeger:
	./scripts/jaeger.sh