SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim example msrv

deps:
	./scripts/install-deps.sh

check:
	typos
	shellcheck ./scripts/*
	./.github/template/generate.sh
	./scripts/minimize-dashboards.sh
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets
	# TODO(MrCroxx): Restore udeps check after it doesn't requires nightly anymore.
	# cargo udeps --workspace --exclude foyer-workspace-hack

check-all:
	shellcheck ./scripts/*
	./.github/template/generate.sh
	./scripts/minimize-dashboards.sh
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets --features deadlock
	cargo clippy --all-targets --features tokio-console
	cargo clippy --all-targets --features trace
	cargo clippy --all-targets
	# TODO(MrCroxx): Restore udeps check after it doesn't requires nightly anymore.
	# cargo udeps --workspace --exclude foyer-workspace-hack

test:
	RUST_BACKTRACE=1 cargo nextest run --all
	RUST_BACKTRACE=1 cargo test --doc

test-ignored:
	RUST_BACKTRACE=1 cargo nextest run --run-ignored ignored-only --no-capture --workspace

test-all: test test-ignored

madsim:
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" cargo clippy --all-targets
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo nextest run --all
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo test --doc

example:
	cargo run --example memory
	cargo run --example hybrid
	cargo run --example hybrid_full

all: check-all test-all example

fast: check test example

msrv:
	shellcheck ./scripts/*
	./.github/template/generate.sh
	./scripts/minimize-dashboards.sh
	cargo +1.76 hakari generate
	cargo +1.76 hakari manage-deps
	cargo +1.76 sort -w
	cargo +1.76 fmt --all
	cargo +1.76 clippy --all-targets --features deadlock
	cargo +1.76 clippy --all-targets --features tokio-console
	cargo +1.76 clippy --all-targets --features trace
	cargo +1.76 clippy --all-targets
	RUST_BACKTRACE=1 cargo +1.76 nextest run --all
	RUST_BACKTRACE=1 cargo +1.76 test --doc
	RUST_BACKTRACE=1 cargo +1.76 nextest run --run-ignored ignored-only --no-capture --workspace

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp
