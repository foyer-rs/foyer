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

use std::hash::{BuildHasher, BuildHasherDefault, Hash};

use twox_hash::XxHash64;

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

/// Code error.
#[derive(Debug, thiserror::Error)]
pub enum CodeError {
    /// The buffer size is not enough to hold the serialized data.
    #[error("exceed size limit")]
    SizeLimit,
    /// Other std io error.
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[cfg(feature = "serde")]
    /// Other bincode error.
    #[error("bincode error: {0}")]
    Bincode(bincode::Error),
    /// Unrecognized.
    #[error("unrecognized data: {0:?}")]
    Unrecognized(Vec<u8>),
    /// Other error.
    #[error("other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
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

#[cfg(feature = "serde")]
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
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError>;

    /// Decode the object from a reader.
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized;

    /// Estimated serialized size of the object.
    ///
    /// The estimated serialized size is used by selector between large and small object disk cache engines.
    fn estimated_size(&self) -> usize;
}

#[cfg(feature = "serde")]
impl<T> Code for T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        bincode::serialize_into(writer, self).map_err(CodeError::from)
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError> {
        bincode::deserialize_from(reader).map_err(CodeError::from)
    }

    fn estimated_size(&self) -> usize {
        bincode::serialized_size(self).unwrap() as usize
    }
}

macro_rules! impl_serde_for_numeric_types {
    ($($t:ty),*) => {
        $(
            #[cfg(not(feature = "serde"))]
            impl Code for $t {
                fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
                    writer.write_all(&self.to_le_bytes()).map_err(CodeError::from)
                }

                fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError> {
                    let mut buf = [0u8; std::mem::size_of::<$t>()];
                    reader.read_exact(&mut buf).map_err(CodeError::from)?;
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

#[cfg(not(feature = "serde"))]
impl Code for bool {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        writer
            .write_all(if *self { &[1u8] } else { &[0u8] })
            .map_err(CodeError::from)
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).map_err(CodeError::from)?;
        match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodeError::Unrecognized(buf.to_vec())),
        }
    }

    fn estimated_size(&self) -> usize {
        1
    }
}

#[cfg(not(feature = "serde"))]
impl Code for Vec<u8> {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        self.len().encode(writer)?;
        writer.write_all(self).map_err(CodeError::from)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe {
            v.set_len(len);
        }
        reader.read_exact(&mut v).map_err(CodeError::from)?;
        Ok(v)
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(not(feature = "serde"))]
impl Code for String {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        self.len().encode(writer)?;
        writer.write_all(self.as_bytes()).map_err(CodeError::from)
    }

    #[expect(clippy::uninit_vec)]
    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut v = Vec::with_capacity(len);
        unsafe {
            v.set_len(len);
        }
        reader.read_exact(&mut v).map_err(CodeError::from)?;
        String::from_utf8(v).map_err(|e| CodeError::Unrecognized(e.into_bytes()))
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of::<usize>() + self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "serde")]
    mod serde {
        use super::*;

        #[test]
        fn test_encode_overflow() {
            let mut buf = [0u8; 4];
            assert!(matches! {1u64.encode(&mut buf.as_mut()), Err(CodeError::SizeLimit)});
        }
    }

    #[cfg(not(feature = "serde"))]
    mod non_serde {
        use super::*;

        #[test]
        fn test_encode_overflow() {
            let mut buf = [0u8; 4];
            assert!(matches! {1u64.encode(&mut buf.as_mut()), Err(CodeError::SizeLimit)});
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
    }
}
