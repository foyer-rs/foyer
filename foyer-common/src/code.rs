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

/// Key trait for the disk cache.
pub trait StorageKey: Key + Serialize + Deserialize {}
impl<T> StorageKey for T where T: Key + Serialize + Deserialize {}

/// Value trait for the disk cache.
pub trait StorageValue: Value + 'static + Serialize + Deserialize {}
impl<T> StorageValue for T where T: Value + Serialize + Deserialize {}

/// Hash builder trait.
pub trait HashBuilder: BuildHasher + Send + Sync + 'static {}
impl<T> HashBuilder for T where T: BuildHasher + Send + Sync + 'static {}

/// Serialization trait for key and value.
pub trait Serialize {
    /// Serialize the object into a writer.
    fn serialize(&self, writer: &mut impl std::io::Write) -> std::io::Result<()>;

    /// Estimated serialized size of the object.
    fn estimated_size(&self) -> usize;
}

impl<T> Serialize for T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn serialize(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        // FIXME(MrCroxx): handle cannot write whole buffer.
        bincode::serialize_into(writer, self).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn estimated_size(&self) -> usize {
        // FIXME(MrCroxx): handle error
        bincode::serialized_size(self).unwrap() as usize
    }
}

/// Deserialization trait for key and value.
pub trait Deserialize {
    /// Deserialize the object from a reader.
    fn deserialize(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized;
}

impl<T> Deserialize for T
where
    T: serde::de::DeserializeOwned,
{
    fn deserialize(reader: &mut impl std::io::Read) -> std::io::Result<Self> {
        // FIXME(MrCroxx): handle cannot read whole buffer.
        bincode::deserialize_from(reader).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}
