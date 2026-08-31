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
import heapq
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
TAG_PATTERN = re.compile(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
CRATES_IO_API = "https://crates.io/api/v1/crates/{name}/{version}"
USER_AGENT = "foyer-release-workflow (https://github.com/foyer-rs/foyer)"


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


def plan_packages(metadata: dict) -> list[str]:
    packages = publishable_packages(metadata)
    dependents = {name: set() for name in packages}
    indegree = {name: 0 for name in packages}

    for name, package in packages.items():
        for dependency in package.get("dependencies", []):
            dependency_name = dependency["name"]
            if dependency.get("kind") == "dev" or dependency.get("path") is None:
                continue
            if dependency_name not in packages or name in dependents[dependency_name]:
                continue
            dependents[dependency_name].add(name)
            indegree[name] += 1

    ready = [name for name, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    ordered = []

    while ready:
        name = heapq.heappop(ready)
        ordered.append(name)
        for dependent in sorted(dependents[name]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heapq.heappush(ready, dependent)

    if len(ordered) != len(packages):
        unresolved = sorted(name for name, degree in indegree.items() if degree > 0)
        raise ReleaseError(
            f"workspace package dependency cycle: {', '.join(unresolved)}"
        )

    return ordered


def version_from_tag(tag: str) -> str:
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ReleaseError(f"release tag must match vMAJOR.MINOR.PATCH: {tag}")
    return tag[1:]


def validate_package_versions(metadata: dict, version: str) -> list[str]:
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
    return plan_packages(metadata)


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


def validate_release(tag: str, commit: str, main_ref: str) -> list[str]:
    version = version_from_tag(tag)
    metadata = load_metadata()
    packages = validate_package_versions(metadata, version)
    validate_changelog(version, PROJECT_DIR / "CHANGELOG.md")
    validate_main_ancestry(commit, main_ref)
    return packages


def crate_version_exists(name: str, version: str) -> bool:
    request = urllib.request.Request(
        CRATES_IO_API.format(name=name, version=version),
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status == 200
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise ReleaseError(
            f"crates.io returned HTTP {error.code} for {name} {version}"
        ) from error
    except urllib.error.URLError as error:
        raise ReleaseError(
            f"failed to query crates.io for {name} {version}: {error}"
        ) from error


def validate_published_prefix(packages: list[str], published: list[bool]) -> None:
    missing_seen = False
    for name, exists in zip(packages, published, strict=True):
        if not exists:
            missing_seen = True
        elif missing_seen:
            raise ReleaseError(
                f"published crate {name} appears after an unpublished dependency in the release plan"
            )


def wait_until_published(name: str, version: str, attempts: int = 30) -> None:
    for attempt in range(attempts):
        if crate_version_exists(name, version):
            return
        if attempt + 1 < attempts:
            time.sleep(10)
    raise ReleaseError(f"timed out waiting for {name} {version} to appear on crates.io")


def is_retryable_publish_failure(output: str) -> bool:
    lowered = output.lower()
    return any(
        message in lowered
        for message in (
            "too many requests",
            "rate limit",
            "you have published too many crates",
            "failed to select a version for the requirement",
            "candidate versions found which didn't match",
        )
    )


def publish_package(name: str, version: str, attempts: int = 3) -> None:
    for attempt in range(attempts):
        print(f"Publishing {name} {version}", flush=True)
        result = run(
            ["cargo", "publish", "--package", name, "--no-verify"],
            capture_output=True,
        )
        output = result.stdout or ""
        print(output, end="", flush=True)

        if result.returncode == 0 or crate_version_exists(name, version):
            wait_until_published(name, version)
            return
        if is_retryable_publish_failure(output) and attempt + 1 < attempts:
            time.sleep(60 * (attempt + 1))
            continue
        raise ReleaseError(f"failed to publish {name} {version}")

    raise ReleaseError(f"failed to publish {name} {version}")


def publish_release(tag: str) -> None:
    if "CARGO_REGISTRY_TOKEN" not in os.environ:
        raise ReleaseError("CARGO_REGISTRY_TOKEN is not set")

    version = version_from_tag(tag)
    metadata = load_metadata()
    packages = validate_package_versions(metadata, version)
    published = [crate_version_exists(name, version) for name in packages]
    validate_published_prefix(packages, published)

    for name, exists in zip(packages, published, strict=True):
        if exists:
            print(f"Skipping {name} {version}: already published", flush=True)
            continue
        publish_package(name, version)


def write_github_output(packages: list[str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path is None:
        raise ReleaseError("GITHUB_OUTPUT is not set")
    with Path(output_path).open("a", encoding="utf-8") as output:
        output.write(f"packages={json.dumps(packages)}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and publish foyer releases")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Print the crate publish order")
    plan_parser.add_argument("--github-output", action="store_true")

    check_parser = subparsers.add_parser("check", help="Validate a release tag")
    check_parser.add_argument("--tag", required=True)
    check_parser.add_argument("--commit", required=True)
    check_parser.add_argument("--main-ref", default="origin/main")

    publish_parser = subparsers.add_parser(
        "publish", help="Publish a release to crates.io"
    )
    publish_parser.add_argument("--tag", required=True)

    args = parser.parse_args()
    if args.command == "plan":
        packages = plan_packages(load_metadata())
        print(json.dumps(packages))
        if args.github_output:
            write_github_output(packages)
    elif args.command == "check":
        packages = validate_release(args.tag, args.commit, args.main_ref)
        print(json.dumps(packages))
    elif args.command == "publish":
        publish_release(args.tag)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReleaseError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
