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

use std::fmt::Debug;

/// Hint for the cache eviction algorithm to decide the priority of the specific entry if needed.
///
/// The meaning of the hint differs in each cache eviction algorithm, and some of them can be ignore by specific
/// algorithm.
///
/// If the given cache hint does not suitable for the cache eviction algorithm that is active, the algorithm may modify
/// it to a proper one.
///
/// For more details, please refer to the document of each enum options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Hint {
    /// The default hint shared by all cache eviction algorithms.
    Normal,
    /// Suggest the priority of the entry is low.
    ///
    /// Used by LRU.
    Low,
}

impl Default for Hint {
    fn default() -> Self {
        Self::Normal
    }
}

// TODO(MrCroxx): Is it necessary to make popluated entry still follow the cache location advice?
/// Advice cache location for the cache entry.
///
/// Useful when using hybrid cache.
///
/// NOTE: `CacheLocation` only affects the first time the entry is handle.
/// After it is populated, the entry may not follow the given advice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Location {
    /// The default location.
    ///
    /// Prefer to store the entry in the in-memory cache with in-memory cache.
    /// And prefer to store the entry in the hybrid cache with hybrid cache.
    #[default]
    Default,
    /// Prefer to store the entry in the in-memory cache.
    InMem,
    /// Prefer to store the entry on the disk cache.
    OnDisk,
}

/// Entry age in the disk cache. Used by hybrid cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Age {
    /// THe entry is still young and will be reserved in the disk cache for a while.
    Young,
    /// The entry is old any will be eviction from the disk cache soon.
    Old,
}

/// Source context for populated entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Populated {
    /// The age of the entry.
    pub age: Age,
}

/// Entry source used by hybrid cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Source {
    /// Comes from outer system of foyer.
    Outer,
    /// Populated from the disk cache.
    Populated(Populated),
}

impl Default for Source {
    fn default() -> Self {
        Self::Outer
    }
}

/// Entry level properties trait.
///
/// The in-memory only cache and the hybrid cache may have different properties implementations to minimize the overhead
/// of necessary properties in different scenarios.
pub trait Properties: Send + Sync + 'static + Clone + Default + Debug {
    /// Set entry as a phantom entry.
    ///
    /// A phantom entry will not be actually inserted into the in-memory cache.
    /// It is only used to keep the APIs consistent.
    ///
    /// NOTE: This API is for internal usage only. It MUST NOT be exported publicly.
    fn with_phantom(self, phantom: bool) -> Self;

    /// If the entry is a phantom entry.
    fn phantom(&self) -> Option<bool>;

    /// Set entry hint.
    fn with_hint(self, hint: Hint) -> Self;

    /// Entry hint.
    fn hint(&self) -> Option<Hint>;

    /// Set entry location.
    fn with_location(self, location: Location) -> Self;

    /// Entry location.
    fn location(&self) -> Option<Location>;

    /// Set entry source.
    fn with_source(self, source: Source) -> Self;

    /// Entry source.
    fn source(&self) -> Option<Source>;
}
