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

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
TAG_PATTERN = re.compile(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


class ReleaseError(RuntimeError):
    pass


def run(
    command: list[str], *, capture_output: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_DIR,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.STDOUT if capture_output else None,
    )


def load_metadata() -> dict:
    result = run(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    if result.returncode != 0:
        raise ReleaseError(f"cargo metadata failed:\n{result.stdout}")
    return json.loads(result.stdout)


def is_publishable(package: dict) -> bool:
    registries = package.get("publish")
    return registries is None or "crates-io" in registries


def publishable_packages(metadata: dict) -> dict[str, dict]:
    return {
        package["name"]: package
        for package in metadata["packages"]
        if is_publishable(package)
    }


def version_from_tag(tag: str) -> str:
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ReleaseError(f"release tag must match vMAJOR.MINOR.PATCH: {tag}")
    return tag[1:]


def validate_package_versions(metadata: dict, version: str) -> None:
    packages = publishable_packages(metadata)
    mismatches = sorted(
        f"{name}={package['version']}"
        for name, package in packages.items()
        if package["version"] != version
    )
    if mismatches:
        raise ReleaseError(
            f"release version {version} does not match publishable packages: "
            + ", ".join(mismatches)
        )


def validate_changelog(version: str, changelog: Path) -> None:
    content = changelog.read_text(encoding="utf-8")
    entry = re.compile(
        rf"^\|\s*foyer\s*\|\s*{re.escape(version)}\s*\|\s*$", re.MULTILINE
    )
    if entry.search(content) is None:
        raise ReleaseError(f"CHANGELOG.md has no foyer {version} release entry")


def validate_main_ancestry(commit: str, main_ref: str) -> None:
    result = run(["git", "merge-base", "--is-ancestor", commit, main_ref])
    if result.returncode != 0:
        raise ReleaseError(f"release commit {commit} is not reachable from {main_ref}")


def validate_release(tag: str, commit: str, main_ref: str) -> None:
    version = version_from_tag(tag)
    metadata = load_metadata()
    validate_package_versions(metadata, version)
    validate_changelog(version, PROJECT_DIR / "CHANGELOG.md")
    validate_main_ancestry(commit, main_ref)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a foyer release")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--main-ref", default="origin/main")

    args = parser.parse_args()
    validate_release(args.tag, args.commit, args.main_ref)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReleaseError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
