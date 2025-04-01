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

use std::hash::{BuildHasher, Hash};

/// Key trait for the in-memory cache.
pub trait Key: Send + Sync + 'static + Hash + Eq {}
/// Value trait for the in-memory cache.
pub trait Value: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + std::hash::Hash + Eq> Key for T {}
impl<T: Send + Sync + 'static> Value for T {}

/// Hash builder trait.
pub trait HashBuilder: BuildHasher + Send + Sync + 'static {}
impl<T> HashBuilder for T where T: BuildHasher + Send + Sync + 'static {}

/// Code error.
#[derive(Debug, thiserror::Error)]
pub enum CodeError {
    /// The buffer size is not enough to hold the serialized data.
    #[error("exceed size limit")]
    SizeLimit,
    /// Other std io error.
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("bincode error: {0}")]
    /// Other bincode error.
    Bincode(bincode::Error),
}

/// Code Result.
pub type CodeResult<T> = std::result::Result<T, CodeError>;

impl From<std::io::Error> for CodeError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::WriteZero => Self::SizeLimit,
            _ => Self::Io(err),
        }
    }
}

impl From<bincode::Error> for CodeError {
    fn from(err: bincode::Error) -> Self {
        match *err {
            bincode::ErrorKind::SizeLimit => Self::SizeLimit,
            bincode::ErrorKind::Io(e) => e.into(),
            _ => Self::Bincode(err),
        }
    }
}

/// Key trait for the disk cache.
pub trait StorageKey: Key + Encode + Decode {}
impl<T> StorageKey for T where T: Key + Encode + Decode {}

/// Value trait for the disk cache.
pub trait StorageValue: Value + 'static + Encode + Decode {}
impl<T> StorageValue for T where T: Value + Encode + Decode {}

/// Encode trait for key and value.
pub trait Encode {
    /// Encode the object into a writer.
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError>;

    /// Estimated serialized size of the object.
    ///
    /// The estimated serialized size is used by selector between large and small object disk cache engines.
    fn estimated_size(&self) -> usize;
}

/// Decode trait for key and value.
pub trait Decode {
    /// Decode the object from a reader.
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized;
}

impl<T> Encode for T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        bincode::serialize_into(writer, self).map_err(CodeError::from)
    }

    fn estimated_size(&self) -> usize {
        bincode::serialized_size(self).unwrap() as usize
    }
}

impl<T> Decode for T
where
    T: serde::de::DeserializeOwned,
{
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError> {
        bincode::deserialize_from(reader).map_err(CodeError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_overflow() {
        let mut buf = [0u8; 4];
        assert!(matches! {1u64.encode(&mut buf.as_mut()), Err(CodeError::SizeLimit)});
    }
}
