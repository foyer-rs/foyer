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

use std::hash::{BuildHasher, BuildHasherDefault, Hash};

use twox_hash::XxHash64;

use crate::error::{Error, Result};

/// Key trait for the in-memory cache.
pub trait Key: Send + Sync + 'static + Hash + Eq {}
/// Value trait for the in-memory cache.
pub trait Value: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + std::hash::Hash + Eq> Key for T {}
impl<T: Send + Sync + 'static> Value for T {}

/// Hash builder trait.
pub trait HashBuilder: BuildHasher + Send + Sync + 'static {}
impl<T> HashBuilder for T where T: BuildHasher + Send + Sync + 'static {}

/// The default hasher for foyer.
///
/// It is guaranteed that the hash results of the same key are the same across different runs.
pub type DefaultHasher = BuildHasherDefault<XxHash64>;

/// Key trait for the disk cache.
pub trait StorageKey: Key + Code {}
impl<T> StorageKey for T where T: Key + Code {}

/// Value trait for the disk cache.
pub trait StorageValue: Value + 'static + Code {}
impl<T> StorageValue for T where T: Value + Code {}

/// Encode/decode trait for key and value.
///
/// [`Code`] is required while working with foyer hybrid cache.
///
/// Some general types has already implemented [`Code`] by foyer, but the user needs to implement it for complex types.
///
/// Or, the user can enable `serde` feature for foyer.
/// Then all types that implements `serde::Serialize` and `serde::de::DeserializeOwned` will be automatically
/// implemented for [`Code`].
pub trait Code {
    /// Encode the object into a writer.
    ///
    /// NOTE:
    ///
    /// When implementing [`Code`], if [`std::io::Error`] or a serialization error occurs during encoding,
    /// please use [`Error::io_error`] or [`Error::postcard_error`] to convert it into [`Error`],
    /// instead of manually creating an [`Error`].
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()>;

    /// Decode the object from a reader.
    ///
    /// NOTE:
    ///
    /// When implementing [`Code`], if [`std::io::Error`] or a serialization error occurs during decoding,
    /// please use [`Error::io_error`] or [`Error::postcard_error`] to convert it into [`Error`],
    /// instead of manually creating an [`Error`].
    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized;

    /// Estimated serialized size of the object.
    ///
    /// The estimated serialized size is used by selector between different disk cache engines.
    fn estimated_size(&self) -> usize;
}

/// Blanket implementation of [`Code`] for types implementing [`borsh::BorshSerialize`] and
/// [`borsh::BorshDeserialize`].
///
/// This takes priority over the `serde`-based blanket implementation when both
/// the `borsh` and `serde` features are enabled.
#[cfg(feature = "borsh")]
impl<T> Code for T
where
    T: borsh::BorshSerialize + borsh::BorshDeserialize,
{
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        borsh::to_writer(writer, self).map_err(Error::io_error)
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self> {
        borsh::from_reader(reader).map_err(Error::io_error)
    }

    fn estimated_size(&self) -> usize {
        borsh::object_length(self).unwrap_or(0)
    }
}

/// Blanket implementation of [`Code`] for types implementing [`serde::Serialize`] and
/// [`serde::de::DeserializeOwned`] via the [`postcard`] serialization format.
///
/// This blanket impl is only active when the `borsh` feature is NOT enabled.
/// When `borsh` is enabled, its blanket impl takes priority to avoid trait coherence conflicts.
#[cfg(all(feature = "serde", not(feature = "borsh")))]
impl<T> Code for T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        postcard::to_io(self, writer).map(|_| ()).map_err(Error::postcard_error)
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).map_err(Error::io_error)?;
        postcard::from_bytes(&buf).map_err(Error::postcard_error)
    }

    fn estimated_size(&self) -> usize {
        postcard::experimental::serialized_size(self).unwrap_or(0)
    }
}

macro_rules! impl_serde_for_numeric_types {
    ($($t:ty),*) => {
        $(
            #[cfg(not(any(feature = "serde", feature = "borsh")))]
            impl Code for $t {
                fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
                    writer.write_all(&self.to_le_bytes()).map_err(Error::io_error)
                }

                fn decode(reader: &mut impl std::io::Read) -> Result<Self> {
                    let mut buf = [0u8; std::mem::size_of::<$t>()];
                    reader.read_exact(&mut buf).map_err(Error::io_error)?;
                    Ok(<$t>::from_le_bytes(buf))
                }

                fn estimated_size(&self) -> usize {
                    std::mem::size_of::<$t>()
                }
            }
        )*
    };
}

macro_rules! for_all_numeric_types {
    ($macro:ident) => {
        $macro! { u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64}
    };
}

for_all_numeric_types! { impl_serde_for_numeric_types }

#[cfg(not(any(feature = "serde", feature = "borsh")))]
impl Code for bool {
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        writer
            .write_all(if *self { &[1u8] } else { &[0u8] })
            .map_err(Error::io_error)
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).map_err(Error::io_error)?;
        match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::new(crate::error::ErrorKind::Parse, "failed to parse bool").with_context("byte", buf[0])),
        }
    }

    fn estimated_size(&self) -> usize {
        1
    }
}

#[cfg(not(any(feature = "serde", feature = "borsh")))]
impl Code for Vec<u8> {
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        self.len().encode(writer)?;
        writer.write_all(self).map_err(Error::io_error)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe {
            v.set_len(len);
        }
        reader.read_exact(&mut v).map_err(Error::io_error)?;
        Ok(v)
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(not(any(feature = "serde", feature = "borsh")))]
impl Code for String {
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        self.len().encode(writer)?;
        writer.write_all(self.as_bytes()).map_err(Error::io_error)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe { v.set_len(len) };
        reader.read_exact(&mut v).map_err(Error::io_error)?;
        String::from_utf8(v)
            .map_err(|e| Error::new(crate::error::ErrorKind::Parse, "failed to parse String").with_source(e))
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(not(any(feature = "serde", feature = "borsh")))]
impl Code for bytes::Bytes {
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        self.len().encode(writer)?;
        writer.write_all(self).map_err(Error::io_error)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe { v.set_len(len) };
        reader.read_exact(&mut v).map_err(Error::io_error)?;
        Ok(bytes::Bytes::from(v))
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests for the `borsh` blanket impl.
    #[cfg(feature = "borsh")]
    mod borsh {
        use super::*;

        #[test]
        fn test_borsh_encode_decode_numeric() {
            // borsh uses io::Error, which maps WriteZero -> BufferSizeLimit
            let mut buf = [0u8; 4];
            let e = 1u64.encode(&mut buf.as_mut()).unwrap_err();
            // A 4-byte buffer is too small for a u64; expect an IO-derived error.
            assert!(
                matches!(
                    e.kind(),
                    crate::error::ErrorKind::BufferSizeLimit | crate::error::ErrorKind::Io
                ),
                "unexpected error kind: {:?}",
                e.kind()
            );
        }

        #[test]
        fn test_borsh_roundtrip() {
            let original = 42u64;
            let mut buf = vec![0xffu8; original.estimated_size()];
            original.encode(&mut buf.as_mut_slice()).unwrap();
            let decoded = u64::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(original, decoded);
        }
    }

    /// Tests for the `postcard` (serde) blanket impl.
    #[cfg(all(feature = "serde", not(feature = "borsh")))]
    mod serde {
        use super::*;

        #[test]
        fn test_postcard_encode_overflow() {
            // postcard uses variable-length encoding; pick a value large enough
            // that it definitely won't fit in a tiny buffer.
            let mut buf = [0u8; 1];
            let e = u64::MAX.encode(&mut buf.as_mut()).unwrap_err();
            // postcard wraps IO errors from the writer as an External error.
            assert!(matches!(e.kind(), crate::error::ErrorKind::External));
        }

        #[test]
        fn test_postcard_roundtrip() {
            let original = 42u64;
            let mut buf = vec![0xffu8; original.estimated_size()];
            original.encode(&mut buf.as_mut_slice()).unwrap();
            let decoded = u64::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(original, decoded);
        }
    }

    /// Tests for manual [`Code`] implementations (neither `serde` nor `borsh`).
    #[cfg(not(any(feature = "serde", feature = "borsh")))]
    mod manual {
        use super::*;

        #[test]
        fn test_encode_overflow() {
            let mut buf = [0u8; 4];
            let e = 1u64.encode(&mut buf.as_mut()).unwrap_err();
            assert_eq!(e.kind(), crate::error::ErrorKind::BufferSizeLimit);
        }

        macro_rules! impl_serde_test_for_numeric_types {
            ($($t:ty),*) => {
                paste::paste! {
                    $(
                        #[test]
                        fn [<test_ $t _serde>]() {
                            for a in [0 as $t, <$t>::MIN, <$t>::MAX] {
                                let mut buf = vec![0xffu8; a.estimated_size()];
                                a.encode(&mut buf.as_mut_slice()).unwrap();
                                let b = <$t>::decode(&mut buf.as_slice()).unwrap();
                                assert_eq!(a, b);
                            }
                        }
                    )*
                }
            };
        }

        for_all_numeric_types! { impl_serde_test_for_numeric_types }

        #[test]
        fn test_bool_serde() {
            let a = true;
            let mut buf = vec![0xffu8; a.estimated_size()];
            a.encode(&mut buf.as_mut_slice()).unwrap();
            let b = bool::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(a, b);
        }

        #[test]
        fn test_vec_u8_serde() {
            let mut a = vec![0u8; 42];
            rand::fill(&mut a[..]);
            let mut buf = vec![0xffu8; a.estimated_size()];
            a.encode(&mut buf.as_mut_slice()).unwrap();
            let b = Vec::<u8>::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(a, b);
        }

        #[test]
        fn test_string_serde() {
            let a = "hello world".to_string();
            let mut buf = vec![0xffu8; a.estimated_size()];
            a.encode(&mut buf.as_mut_slice()).unwrap();
            let b = String::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(a, b);
        }

        #[test]
        fn test_bytes_serde() {
            let mut a = vec![0u8; 42];
            rand::fill(&mut a[..]);
            let a = bytes::Bytes::from(a);
            let mut buf = vec![0xffu8; a.estimated_size()];
            a.encode(&mut buf.as_mut_slice()).unwrap();
            let b = Vec::<u8>::decode(&mut buf.as_slice()).unwrap();
            assert_eq!(a, b);
        }
    }
}
