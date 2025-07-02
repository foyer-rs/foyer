SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim example msrv udeps ffmt machete misc

misc:
	typos
	shellcheck ./scripts/*
	./scripts/minimize-dashboards.sh

udeps:
	RUSTFLAGS="--cfg tokio_unstable -Awarnings" cargo +nightly-2024-08-30 udeps --all-targets

machete:
	cargo machete

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp
