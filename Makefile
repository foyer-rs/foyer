SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim example msrv udeps ffmt

deps:
	./scripts/install-deps.sh

check:
	typos
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets --features prometheus,opentelemetry

check-all:
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets --features deadlock
	cargo clippy --all-targets --features tokio-console
	cargo clippy --all-targets --features sanity
	cargo clippy --all-targets --features tracing
	cargo clippy --all-targets --features prometheus,opentelemetry
	cargo clippy --all-targets

test:
	RUST_BACKTRACE=1 cargo nextest run --all --features "strict_assertions,sanity,prometheus,opentelemetry"
	RUST_BACKTRACE=1 cargo test --doc

test-ignored:
	RUST_BACKTRACE=1 cargo nextest run --run-ignored ignored-only --no-capture --workspace --features "strict_assertions,sanity"

test-all: test test-ignored

madsim:
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" cargo clippy --all-targets
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo nextest run --all --features "strict_assertions,sanity"

example:
	cargo run --example memory
	cargo run --example hybrid
	cargo run --example hybrid_full
	cargo run --example event_listener
	cargo run --features "tracing,jaeger" --example tail_based_tracing
	cargo run --features "tracing,ot" --example tail_based_tracing
	cargo run --example equivalent

full: check-all test-all example udeps

fast: check test example

msrv:
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh
	cargo +1.82.0 sort -w
	cargo +1.82.0 fmt --all
	cargo +1.82.0 clippy --all-targets --features deadlock
	cargo +1.82.0 clippy --all-targets --features tokio-console
	cargo +1.82.0 clippy --all-targets
	RUST_BACKTRACE=1 cargo +1.82.0 nextest run --all
	RUST_BACKTRACE=1 cargo +1.82.0 test --doc
	RUST_BACKTRACE=1 cargo +1.82.0 nextest run --run-ignored ignored-only --no-capture --workspace

udeps:
	RUSTFLAGS="--cfg tokio_unstable -Awarnings" cargo +nightly-2024-08-30 udeps --all-targets

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp

ffmt:
	cargo +nightly fmt --all -- --config-path rustfmt.nightly.toml