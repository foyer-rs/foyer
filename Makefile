SHELL := /bin/bash
.PHONY: proto check test

check:
	cargo sort -w && cargo fmt --all && cargo clippy --all-targets

test:
	cargo test --all