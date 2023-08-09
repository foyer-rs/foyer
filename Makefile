SHELL := /bin/bash
.PHONY: proto check test deps

deps:
	./scripts/install-deps.sh

check:
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets

test:
	RUST_BACKTRACE=1 cargo test --all