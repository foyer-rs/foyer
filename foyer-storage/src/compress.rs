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

// TODO(MrCroxx): unify compress interface?

use crate::error::Error;

/// The compression algorithm of the disk cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum Compression {
    /// No compression enabled.
    #[default]
    None,
    /// Use zstd compression.
    Zstd,
    /// Use lz4 compression.
    Lz4,
}

impl Compression {
    /// Get the u8 that represent the compression algorithm.
    pub fn to_u8(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
        }
    }
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Zstd => 1,
            Compression::Lz4 => 2,
        }
    }
}

impl TryFrom<u8> for Compression {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::Lz4),
            _ => Err(Error::CompressionAlgorithmNotSupported(value)),
        }
    }
}
