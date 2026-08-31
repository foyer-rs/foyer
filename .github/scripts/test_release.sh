#!/usr/bin/env bash

# Copyright 2026 foyer Project Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/release.sh"

RELEASE_TEST_TEMP_DIR="$(mktemp -d)"
readonly RELEASE_TEST_TEMP_DIR

cleanup() {
  rm -rf -- "${RELEASE_TEST_TEMP_DIR}"
}
trap cleanup EXIT

assert_eq() {
  local expected="$1"
  local actual="$2"

  if [[ "${expected}" != "${actual}" ]]; then
    echo "expected: ${expected}" >&2
    echo "actual:   ${actual}" >&2
    return 1
  fi
}

assert_fails() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    return 1
  fi
}

test_version_from_tag() {
  assert_eq "1.2.3" "$(version_from_tag "v1.2.3")"
  assert_fails version_from_tag "v1.2.3-rc.1"
  assert_fails version_from_tag "v01.2.3"
  assert_fails version_from_tag "1.2.3"
}

test_publishable_versions() {
  local metadata
  metadata='{
    "packages": [
      {
        "name": "foyer",
        "version": "1.2.4",
        "publish": null
      },
      {
        "name": "foyer-common",
        "version": "1.2.3",
        "publish": ["crates-io"]
      },
      {
        "name": "xtask",
        "version": "1.2.4",
        "publish": []
      },
      {
        "name": "private",
        "version": "1.2.4",
        "publish": ["private-registry"]
      }
    ]
  }'

  assert_eq \
    "foyer=1.2.4" \
    "$(publishable_version_mismatches "1.2.3" <<<"${metadata}")"
}

test_changelog() {
  local changelog="${RELEASE_TEST_TEMP_DIR}/CHANGELOG.md"

  printf '| foyer | 1.2.3 |\n' >"${changelog}"
  validate_changelog "1.2.3" "${changelog}"
  assert_fails validate_changelog "1.2.4" "${changelog}"
}

test_version_from_tag
test_publishable_versions
test_changelog

echo "All release helper tests passed"
