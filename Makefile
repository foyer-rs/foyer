SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim example msrv udeps ffmt machete misc

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp
