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

//! Exclusive relative namespace prefix for the first-version OpenDAL cache.
//!
//! Object paths are `{namespace}/...` under the caller-supplied operator root.
//! This check only rejects prefixes that could leave that relative tree. Actual
//! exclusivity is the caller's fresh-namespace contract: one owner, unused
//! prefix, [`foyer::RecoverMode::None`]. There is no distributed lock, existence
//! probe, restart recovery, shared writer, or automatic orphan scan.
//!
//! Residual objects remain after a normal close, skipped cleanup, crash,
//! timed-out write, or failed delete. The caller or operator reclaims them.
//! A restart must use a new unused prefix and starts with an empty in-process
//! index.

use foyer::{Error, ErrorKind, Result};

/// Accept a nonempty relative prefix that cannot traverse above itself.
pub(crate) fn validate(namespace: &str) -> Result<()> {
    if !is_relative_exclusive_prefix(namespace) {
        return Err(Error::new(
            ErrorKind::Config,
            "cache namespace must be a nonempty relative path without '.' or '..' segments",
        ));
    }
    Ok(())
}

fn is_relative_exclusive_prefix(namespace: &str) -> bool {
    if namespace.is_empty() || namespace != namespace.trim() {
        return false;
    }
    if namespace.as_bytes().contains(&0) || namespace.contains('\\') {
        return false;
    }
    namespace
        .split('/')
        .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}
