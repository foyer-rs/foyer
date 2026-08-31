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

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
SCRIPT_PATH = Path(__file__).with_name("release.py")
SPEC = importlib.util.spec_from_file_location("release", SCRIPT_PATH)
release = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(release)


def package(name, version="1.2.3", publish=None, dependencies=None):
    return {
        "name": name,
        "version": version,
        "publish": publish,
        "dependencies": dependencies or [],
    }


def local_dependency(name, kind=None):
    return {"name": name, "kind": kind, "path": f"/{name}"}


class ReleaseTest(unittest.TestCase):
    def test_plan_packages_uses_dependency_order(self):
        metadata = {
            "packages": [
                package("foyer", dependencies=[local_dependency("foyer-storage")]),
                package("foyer-common", dependencies=[local_dependency("foyer-tokio")]),
                package(
                    "foyer-storage", dependencies=[local_dependency("foyer-common")]
                ),
                package("foyer-tokio"),
                package("xtask", publish=[]),
            ]
        }

        self.assertEqual(
            release.plan_packages(metadata),
            ["foyer-tokio", "foyer-common", "foyer-storage", "foyer"],
        )

    def test_plan_packages_ignores_dev_dependencies(self):
        metadata = {
            "packages": [
                package(
                    "foyer", dependencies=[local_dependency("foyer-common", "dev")]
                ),
                package("foyer-common", dependencies=[local_dependency("foyer")]),
            ]
        }

        self.assertEqual(release.plan_packages(metadata), ["foyer", "foyer-common"])

    def test_plan_packages_rejects_dependency_cycles(self):
        metadata = {
            "packages": [
                package("foyer", dependencies=[local_dependency("foyer-common")]),
                package("foyer-common", dependencies=[local_dependency("foyer")]),
            ]
        }

        with self.assertRaisesRegex(release.ReleaseError, "dependency cycle"):
            release.plan_packages(metadata)

    def test_version_from_tag_accepts_stable_semver(self):
        self.assertEqual(release.version_from_tag("v1.2.3"), "1.2.3")

    def test_version_from_tag_rejects_prerelease(self):
        with self.assertRaisesRegex(release.ReleaseError, "vMAJOR.MINOR.PATCH"):
            release.version_from_tag("v1.2.3-rc.1")

    def test_validate_package_versions_rejects_mismatch(self):
        metadata = {"packages": [package("foyer", version="1.2.4")]}

        with self.assertRaisesRegex(release.ReleaseError, "foyer=1.2.4"):
            release.validate_package_versions(metadata, "1.2.3")

    def test_publishable_packages_respects_registry_restrictions(self):
        metadata = {
            "packages": [
                package("foyer"),
                package("foyer-common", publish=["crates-io"]),
                package("internal", publish=[]),
                package("private", publish=["private-registry"]),
            ]
        }

        self.assertEqual(
            sorted(release.publishable_packages(metadata)),
            ["foyer", "foyer-common"],
        )

    def test_validate_changelog_requires_release_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            changelog = Path(directory) / "CHANGELOG.md"
            changelog.write_text("| foyer | 1.2.3 |\n", encoding="utf-8")
            release.validate_changelog("1.2.3", changelog)

            with self.assertRaisesRegex(release.ReleaseError, "no foyer 1.2.4"):
                release.validate_changelog("1.2.4", changelog)

    def test_validate_published_prefix_accepts_partial_release(self):
        release.validate_published_prefix(
            ["foyer-common", "foyer-storage", "foyer"],
            [True, True, False],
        )

    def test_validate_published_prefix_rejects_gap(self):
        with self.assertRaisesRegex(release.ReleaseError, "unpublished dependency"):
            release.validate_published_prefix(
                ["foyer-common", "foyer-storage", "foyer"],
                [True, False, True],
            )

    def test_registry_propagation_failure_is_retryable(self):
        self.assertTrue(
            release.is_retryable_publish_failure(
                "failed to select a version for the requirement `foyer-common = ^1.2.3`"
            )
        )


if __name__ == "__main__":
    unittest.main()
