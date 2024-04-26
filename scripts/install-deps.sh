#!/bin/bash

if [ -z "$(which cargo-binstall)" ]; then
    curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
fi

cargo binstall -y cargo-sort cargo-nextest typos-cli