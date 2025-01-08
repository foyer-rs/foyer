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

use std::{
    fmt::{Debug, Display},
    ops::Range,
};

/// Disk cache error type.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding/decoding error with `bincode`.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    /// Multiple error list.
    Multiple(MultipleError),
    /// Entry magic mismatch.
    #[error("magic mismatch, expected: {expected}, get: {get}")]
    MagicMismatch {
        /// Expected magic.
        expected: u32,
        /// Gotten magic.
        get: u32,
    },
    /// Entry checksum mismatch.
    #[error("checksum mismatch, expected: {expected}, get: {get}")]
    ChecksumMismatch {
        /// Expected checksum.
        expected: u64,
        /// Gotten checksum.
        get: u64,
    },
    /// Out of range.
    #[error("out of range, valid: {valid:?}, get: {get:?}")]
    OutOfRange {
        /// Valid range.
        valid: Range<usize>,
        /// Gotten range.
        get: Range<usize>,
    },
    /// Compression algorithm not supported.
    #[error("compression algorithm not supported: {0}")]
    CompressionAlgorithmNotSupported(u8),
    /// Other error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Combine multiple errors into one error.
    pub fn multiple(errs: Vec<Error>) -> Self {
        Self::Multiple(MultipleError(errs))
    }
}

#[derive(thiserror::Error, Debug)]
pub struct MultipleError(Vec<Error>);

impl Display for MultipleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multiple errors: [")?;
        if let Some((last, errs)) = self.0.as_slice().split_last() {
            for err in errs {
                write!(f, "{}, ", err)?;
            }
            write!(f, "{}", last)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

/// Disk cache result type.
pub type Result<T> = core::result::Result<T, Error>;
