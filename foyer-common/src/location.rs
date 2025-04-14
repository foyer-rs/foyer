// Copyright 2025 foyer Project Authors
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

/// Advice cache location for the cache entry.
///
/// Useful when using hybrid cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLocation {
    /// The default location.
    ///
    /// Prefer to store the entry in the in-memory cache with in-memory cache.
    /// And prefer to store the entry in the hybrid cache with hybrid cache.
    Default,
    /// Prefer to store the entry in the in-memory cache only.
    InMem,
    /// Prefer to store the entry on the disk cache only.
    ///
    /// TODO(MrCroxx): This option is not supported yet. The behaviour is the same as `Default`.
    OnDisk,
}

impl Default for CacheLocation {
    fn default() -> Self {
        Self::Default
    }
}
