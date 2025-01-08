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

/// Disk cache error type.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// `fio` not available.
    #[error("fio not available")]
    FioNotAvailable,
    /// Other error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Disk cache result type.
pub type Result<T> = core::result::Result<T, Error>;
