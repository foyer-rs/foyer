// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{fs, process::Command};

use clap::Parser;
use serde_json::Value;

#[derive(Debug, Parser)]
pub struct ReleaseCheckArgs {
    /// Release tag in vMAJOR.MINOR.PATCH format.
    #[arg(long)]
    tag: String,

    /// Commit referenced by the release tag.
    #[arg(long)]
    commit: String,

    /// Main branch reference used for ancestry validation.
    #[arg(long, default_value = "origin/main")]
    main_ref: String,
}

pub fn run(args: ReleaseCheckArgs) {
    if let Err(error) = validate_release(&args) {
        eprintln!("Release check failed: {error}");
        std::process::exit(1);
    }
}

fn validate_release(args: &ReleaseCheckArgs) -> Result<(), String> {
    let version = version_from_tag(&args.tag)?;
    let metadata = load_metadata()?;
    validate_package_versions(&metadata, version)?;

    let changelog =
        fs::read_to_string("CHANGELOG.md").map_err(|error| format!("failed to read CHANGELOG.md: {error}"))?;
    validate_changelog(&changelog, version)?;
    validate_main_ancestry(&args.commit, &args.main_ref)
}

fn version_from_tag(tag: &str) -> Result<&str, String> {
    let Some(version) = tag.strip_prefix('v') else {
        return Err(format!("release tag must match vMAJOR.MINOR.PATCH: {tag}"));
    };
    let components = version.split('.').collect::<Vec<_>>();
    if components.len() != 3
        || components.iter().any(|component| {
            component.is_empty()
                || !component.bytes().all(|byte| byte.is_ascii_digit())
                || (component.len() > 1 && component.starts_with('0'))
        })
    {
        return Err(format!("release tag must match vMAJOR.MINOR.PATCH: {tag}"));
    }
    Ok(version)
}

fn load_metadata() -> Result<Value, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .map_err(|error| format!("failed to run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| format!("failed to parse cargo metadata: {error}"))
}

fn validate_package_versions(metadata: &Value, version: &str) -> Result<(), String> {
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| "cargo metadata has no packages array".to_string())?;
    let mut mismatches = Vec::new();

    for package in packages {
        if !is_publishable(package)? {
            continue;
        }
        let name = package
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "cargo metadata package has no name".to_string())?;
        let package_version = package
            .get("version")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("cargo metadata package {name} has no version"))?;
        if package_version != version {
            mismatches.push(format!("{name}={package_version}"));
        }
    }

    if mismatches.is_empty() {
        return Ok(());
    }
    mismatches.sort();
    Err(format!(
        "release version {version} does not match publishable packages: {}",
        mismatches.join(", ")
    ))
}

fn is_publishable(package: &Value) -> Result<bool, String> {
    match package.get("publish") {
        None | Some(Value::Null) => Ok(true),
        Some(Value::Array(registries)) => Ok(registries.iter().any(|registry| registry.as_str() == Some("crates-io"))),
        Some(_) => Err("cargo metadata package has an invalid publish field".to_string()),
    }
}

fn validate_changelog(changelog: &str, version: &str) -> Result<(), String> {
    let found = changelog.lines().any(|line| {
        let line = line.trim();
        let Some(columns) = line.strip_prefix('|').and_then(|line| line.strip_suffix('|')) else {
            return false;
        };
        let columns = columns.split('|').map(str::trim).collect::<Vec<_>>();
        columns.as_slice() == ["foyer", version]
    });

    if found {
        Ok(())
    } else {
        Err(format!("CHANGELOG.md has no foyer {version} release entry"))
    }
}

fn validate_main_ancestry(commit: &str, main_ref: &str) -> Result<(), String> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", commit, main_ref])
        .status()
        .map_err(|error| format!("failed to run git merge-base: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("release commit {commit} is not reachable from {main_ref}"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn version_from_tag_accepts_stable_version() {
        assert_eq!(version_from_tag("v1.2.3").unwrap(), "1.2.3");
    }

    #[test]
    fn version_from_tag_rejects_invalid_versions() {
        for tag in ["1.2.3", "v1.2", "v1.2.3-rc.1", "v01.2.3"] {
            assert!(version_from_tag(tag).is_err(), "accepted invalid tag {tag}");
        }
    }

    #[test]
    fn package_versions_only_include_crates_io_packages() {
        let metadata = json!({
            "packages": [
                { "name": "foyer", "version": "1.2.4", "publish": null },
                { "name": "foyer-common", "version": "1.2.3", "publish": ["crates-io"] },
                { "name": "xtask", "version": "1.2.4", "publish": [] },
                { "name": "private", "version": "1.2.4", "publish": ["private-registry"] },
            ]
        });

        assert_eq!(
            validate_package_versions(&metadata, "1.2.3").unwrap_err(),
            "release version 1.2.3 does not match publishable packages: foyer=1.2.4"
        );
    }

    #[test]
    fn changelog_requires_foyer_release_row() {
        validate_changelog("| foyer | 1.2.3 |\n", "1.2.3").unwrap();
        assert!(validate_changelog("| foyer-common | 1.2.3 |\n", "1.2.3").is_err());
    }
}
