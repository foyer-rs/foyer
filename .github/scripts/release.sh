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

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly PROJECT_DIR

version_from_tag() {
  local tag="$1"

  if [[ ! "${tag}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo "error: release tag must match vMAJOR.MINOR.PATCH: ${tag}" >&2
    return 1
  fi

  printf '%s\n' "${tag#v}"
}

publishable_version_mismatches() {
  local version="$1"

  jq -r --arg version "${version}" '
    .packages[]
    | select(
        .publish == null
        or ((.publish // []) | index("crates-io") != null)
      )
    | select(.version != $version)
    | "\(.name)=\(.version)"
  '
}

validate_changelog() {
  local version="$1"
  local changelog="$2"
  local escaped_version="${version//./\\.}"

  if ! grep -Eq \
    "^[[:space:]]*\\|[[:space:]]*foyer[[:space:]]*\\|[[:space:]]*${escaped_version}[[:space:]]*\\|[[:space:]]*$" \
    "${changelog}"; then
    echo "error: CHANGELOG.md has no foyer ${version} release entry" >&2
    return 1
  fi
}

validate_main_ancestry() {
  local commit="$1"
  local main_ref="$2"

  if ! git merge-base --is-ancestor "${commit}" "${main_ref}"; then
    echo "error: release commit ${commit} is not reachable from ${main_ref}" >&2
    return 1
  fi
}

main() {
  if (( $# < 2 || $# > 3 )); then
    echo "usage: release.sh TAG COMMIT [MAIN_REF]" >&2
    return 2
  fi

  local tag="$1"
  local commit="$2"
  local main_ref="${3:-origin/main}"
  local version
  local metadata
  local mismatches

  cd "${PROJECT_DIR}"

  version="$(version_from_tag "${tag}")"
  metadata="$(cargo metadata --no-deps --format-version 1)"
  mismatches="$(publishable_version_mismatches "${version}" <<<"${metadata}")"

  if [[ -n "${mismatches}" ]]; then
    echo "error: release version ${version} does not match publishable packages:" >&2
    echo "${mismatches}" >&2
    return 1
  fi

  validate_changelog "${version}" "${PROJECT_DIR}/CHANGELOG.md"
  validate_main_ancestry "${commit}" "${main_ref}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
