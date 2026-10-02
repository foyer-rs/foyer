#!/usr/bin/env bash
# One-off verification for the fetch_update MSRV fix. Not part of the product change.
set -u

CODE_DIR=${1:?code checkout}
LOG=${2:?log file}
EXPECTED_SHA=${3:?expected commit}

mkdir -p "$(dirname "$LOG")"
: >"$LOG"

cd "$CODE_DIR"

actual_sha=$(git rev-parse HEAD)
{
    echo "DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "EXPECTED_SHA=$EXPECTED_SHA"
    echo "HEAD=$actual_sha"
    echo "STATUS<<EOF"
    git status --porcelain
    echo "EOF"
    echo "RUSTC_1_91=$(rustc +1.91.0 --version)"
    echo "RUSTC_STABLE=$(rustc +stable --version)"
    echo "MEM=$(free -h | awk '/^Mem:/ {print $2, $3, $7}')"
} >>"$LOG"

if [[ "$actual_sha" != "$EXPECTED_SHA" ]]; then
    echo "EXIT:2 sha mismatch" | tee -a "$LOG"
    exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "EXIT:2 dirty checkout" | tee -a "$LOG"
    exit 2
fi

run() {
    local toolchain=$1
    shift
    echo "CMD: cargo +${toolchain} $*" | tee -a "$LOG"
    CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR="${CODE_DIR}/target-${toolchain}" \
        cargo +"$toolchain" "$@" >>"$LOG" 2>&1
    local code=$?
    echo "EXIT:${code}" | tee -a "$LOG"
    return "$code"
}

# MSRV 1.91: compare_exchange_weak loops compile, and clippy stays clean.
run 1.91.0 fmt --all -- --check || exit $?
run 1.91.0 clippy -p foyer-opendal --all-targets --features test-redis -- -D warnings || exit $?
run 1.91.0 clippy --all-targets --features tokio-console -- -D warnings -A clippy::large_enum_variant || exit $?
run 1.91.0 clippy --all-targets -- -D warnings || exit $?
run 1.91.0 test -p foyer-memory --lib eviction::s3fifo -- --test-threads=8 || exit $?
run 1.91.0 test -p foyer-opendal --lib -- --test-threads=8 || exit $?

# Same commands as the stable CI clippy step, both serde matrix cells.
run stable fmt --all -- --check || exit $?
run stable clippy -p foyer || exit $?
run stable clippy -p foyer --features tracing || exit $?
run stable clippy --all-targets --features tokio-console -- -D warnings -A clippy::large_enum_variant || exit $?
run stable clippy --all-targets --features deadlock -- -D warnings || exit $?
run stable clippy --all-targets --features tracing -- -D warnings || exit $?
run stable clippy --all-targets --features clap -- -D warnings || exit $?
run stable clippy --all-targets -- -D warnings || exit $?
run stable clippy --all-targets --features serde --features tokio-console -- -D warnings -A clippy::large_enum_variant || exit $?
run stable clippy --all-targets --features serde --features deadlock -- -D warnings || exit $?
run stable clippy --all-targets --features serde --features tracing -- -D warnings || exit $?
run stable clippy --all-targets --features serde --features clap -- -D warnings || exit $?
run stable clippy --all-targets --features serde -- -D warnings || exit $?

# The opendal-redis job is stable-only.
run stable clippy -p foyer-opendal --all-targets --features test-redis -- -D warnings || exit $?
run stable test -p foyer-memory --lib eviction::s3fifo -- --test-threads=8 || exit $?
run stable test -p foyer-opendal --lib -- --test-threads=8 || exit $?

echo "ALL_OK" | tee -a "$LOG"
