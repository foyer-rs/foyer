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

/// I/O related error from foyer disk cache io module.
#[derive(Debug, thiserror::Error)]
pub enum IoError {
    /// Std I/O error.
    #[error("I/O operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Customized I/O error.
    #[error("No space left on device: capacity={capacity}, allocated={allocated}, required={required}")]
    NoSpace {
        /// Device capacity in bytes.
        capacity: usize,
        /// Device allocated bytes.
        allocated: usize,
        /// Required bytes for the operation.
        required: usize,
    },
    /// Other I/O error/
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl IoError {
    /// Convert raw OS error to io error.
    pub fn from_raw_os_error(raw: i32) -> Self {
        Self::Io(std::io::Error::from_raw_os_error(raw))
    }

    /// Convert customized error to io error.
    pub fn other<E>(e: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::Other(e.into())
    }
}

/// I/O related result type.
pub type IoResult<T> = std::result::Result<T, IoError>;
