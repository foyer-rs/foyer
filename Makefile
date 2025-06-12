SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim example msrv udeps ffmt machete misc

deps:
	./scripts/install-deps.sh

misc:
	typos
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh

check:
	cargo sort -w
	taplo fmt
	cargo fmt --all
	cargo clippy --all-targets

check-all:
	cargo sort -w
	taplo fmt
	cargo fmt --all
	cargo clippy --all-targets --features deadlock
	cargo clippy --all-targets --features tokio-console -- -A "clippy::large_enum_variant"
	cargo clippy --all-targets --features tracing
	cargo clippy --all-targets --features serde
	cargo clippy --all-targets --features clap
	cargo clippy --all-targets

test:
	RUST_BACKTRACE=1 cargo nextest run --all --features "strict_assertions"
	RUST_BACKTRACE=1 cargo test --doc
	RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --features "nightly" --no-deps

test-ignored:
	RUST_BACKTRACE=1 cargo nextest run --run-ignored ignored-only --no-capture --workspace --features "strict_assertions"

test-all: test test-ignored

madsim:
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" cargo clippy --all-targets
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo nextest run --all --features "strict_assertions"

example:
	cargo run --example memory
	cargo run --example hybrid
	cargo run --example hybrid_full
	cargo run --example event_listener
	cargo run --features "tracing,jaeger" --example tail_based_tracing
	cargo run --features "tracing,ot" --example tail_based_tracing
	cargo run --example equivalent
	cargo run --example export_metrics_prometheus_hyper
	cargo run --features serde --example serde

msrv:
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh
	cargo +1.81.0 sort -w
	cargo +1.81.0 fmt --all
	cargo +1.81.0 clippy --all-targets --features deadlock
	cargo +1.81.0 clippy --all-targets --features tokio-console
	cargo +1.81.0 clippy --all-targets
	RUST_BACKTRACE=1 cargo +1.81.0 nextest run --all
	RUST_BACKTRACE=1 cargo +1.81.0 test --doc
	RUST_BACKTRACE=1 cargo +1.81.0 nextest run --run-ignored ignored-only --no-capture --workspace

udeps:
	RUSTFLAGS="--cfg tokio_unstable -Awarnings" cargo +nightly-2024-08-30 udeps --all-targets

machete:
	cargo machete

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp

ffmt:
	cargo +nightly fmt --all -- --config-path rustfmt.nightly.toml

all: misc ffmt check-all test-all example machete udeps

fast: misc ffmt check test example machete