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

/// Errors enum for foyer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// foyer in-memory cache error.
    #[error("foyer memory error: {0}")]
    Memory(#[from] foyer_memory::Error),
    /// foyer disk cache error.
    #[error("foyer storage error: {0}")]
    Storage(#[from] foyer_storage::Error),
    #[error("other error: {0}")]
    /// Other error.
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Error {
    /// Create customized error.
    pub fn other<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Other(Box::new(err))
    }
}

/// Result type for foyer.
pub type Result<T> = std::result::Result<T, Error>;
