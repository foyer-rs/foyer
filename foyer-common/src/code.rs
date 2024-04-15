//  Copyright 2024 Foyer Project Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use crate::buf::BufExt;
use bytes::{Buf, BufMut};
use paste::paste;

pub trait Key: Send + Sync + 'static + Hash + Eq + PartialEq + Ord + PartialOrd {}
pub trait Value: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + std::hash::Hash + Eq + Ord> Key for T {}
impl<T: Send + Sync + 'static> Value for T {}

pub type CodingError = anyhow::Error;
pub type CodingResult<T> = Result<T, CodingError>;

pub trait Cursor<T>: Send + Sync + 'static + std::io::Read + Debug {
    fn into_inner(self) -> T;
}

/// [`StorageKey`] is required to implement [`Clone`].
///
/// If cloning a [`StorageKey`] is expensive, wrap it with [`Arc`].
// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(unused_variables)]
pub trait StorageKey: Key + Debug + Clone {
    // TODO(MrCroxx): Restore this after `associated_type_defaults` is stable.
    // type Cursor: Cursor<Self> = UnimplementedCursor<Self>;
    type Cursor: Cursor<Self>;

    /// memory weight
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Key` if storage is used.")
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        panic!("Method `read` must be implemented for `Key` if storage is used.")
    }

    fn into_cursor(self) -> Self::Cursor {
        panic!("Associated type `Cursor` and method `into_cursor` must be implemented for `Key` if storage is used.")
    }
}

/// [`StorageValue`] is required to implement [`Clone`].
///
/// If cloning a [`StorageValue`] is expensive, wrap it with [`Arc`].
// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(unused_variables)]
pub trait StorageValue: Value + 'static + Debug + Clone {
    // TODO(MrCroxx): Restore this after `associated_type_defaults` is stable.
    // type Cursor: Cursor<Self> = UnimplementedCursor<Self>;
    type Cursor: Cursor<Self>;

    /// memory weight
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Value` if storage is used.")
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        panic!("Method `read` must be implemented for `Value` if storage is used.")
    }

    fn into_cursor(self) -> Self::Cursor {
        panic!("Associated type `Cursor` and method `into_cursor` must be implemented for `Value` if storage is used.")
    }
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro! {
            {u8, U8},
            {u16, U16},
            {u32, U32},
            {u64, U64},
            {usize, Usize},
            {i8, I8},
            {i16, I16},
            {i32, I32},
            {i64, I64},
            {isize, Isize},
        }
    };
}

macro_rules! def_cursor {
    ($( { $type:ty, $id:ident }, )*) => {
        paste! {
            $(
                #[derive(Debug)]
                pub struct [<PrimitiveCursor $id>] {
                    inner: $type,
                    pos: u8,
                }

                impl [<PrimitiveCursor $id>] {
                    pub fn new(inner: $type) -> Self {
                        Self {
                            inner,
                            pos: 0,
                        }
                    }
                }

                impl std::io::Read for [<PrimitiveCursor $id>] {
                    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
                        let slice = self.inner.to_be_bytes();
                        let len = std::cmp::min(slice.len() - self.pos as usize, buf.len());
                        buf.put_slice(&slice[self.pos as usize..self.pos as usize + len]);
                        self.pos += len as u8;
                        Ok(len)
                    }
                }

                impl Cursor<$type> for [<PrimitiveCursor $id>] {
                    fn into_inner(self) -> $type {
                        self.inner
                    }
                }
            )*
        }
    };
}

macro_rules! impl_key {
    ($( { $type:ty, $id:ident }, )*) => {
        paste! {
            $(
                impl StorageKey for $type {
                    type Cursor = [<PrimitiveCursor $id>];

                    fn serialized_len(&self) -> usize {
                        std::mem::size_of::<$type>()
                    }

                    fn read(mut buf: &[u8]) -> CodingResult<Self> {
                        Ok(buf.[< get_ $type>]())
                    }

                    fn into_cursor(self) -> Self::Cursor {
                        [<PrimitiveCursor $id>]::new(self)
                    }
                }
            )*
        }
    };
}

macro_rules! impl_value {
    ($( { $type:ty, $id:ident }, )*) => {
        paste! {
            $(
                impl StorageValue for $type {
                    type Cursor = [<PrimitiveCursor $id>];

                    fn serialized_len(&self) -> usize {
                        std::mem::size_of::<$type>()
                    }

                    fn read(mut buf: &[u8]) -> CodingResult<Self> {
                        Ok(buf.[< get_ $type>]())
                    }

                    fn into_cursor(self) -> Self::Cursor {
                        [<PrimitiveCursor $id>]::new(self)
                    }
                }
            )*
        }
    };
}

for_all_primitives! { def_cursor }
for_all_primitives! { impl_key }
for_all_primitives! { impl_value }

impl StorageKey for Vec<u8> {
    type Cursor = std::io::Cursor<Vec<u8>>;

    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(buf.to_vec())
    }

    fn into_cursor(self) -> Self::Cursor {
        std::io::Cursor::new(self)
    }
}

impl StorageValue for Vec<u8> {
    type Cursor = std::io::Cursor<Vec<u8>>;

    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(buf.to_vec())
    }

    fn into_cursor(self) -> Self::Cursor {
        std::io::Cursor::new(self)
    }
}

impl Cursor<Vec<u8>> for std::io::Cursor<Vec<u8>> {
    fn into_inner(self) -> Vec<u8> {
        self.into_inner()
    }
}

impl StorageKey for Arc<Vec<u8>> {
    type Cursor = ArcVecU8Cursor;

    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(Arc::new(buf.to_vec()))
    }

    fn into_cursor(self) -> Self::Cursor {
        ArcVecU8Cursor::new(self)
    }
}

impl StorageValue for Arc<Vec<u8>> {
    type Cursor = ArcVecU8Cursor;

    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(Arc::new(buf.to_vec()))
    }

    fn into_cursor(self) -> Self::Cursor {
        ArcVecU8Cursor::new(self)
    }
}

#[derive(Debug)]
pub struct ArcVecU8Cursor {
    inner: Arc<Vec<u8>>,
    pos: usize,
}

impl ArcVecU8Cursor {
    pub fn new(inner: Arc<Vec<u8>>) -> Self {
        Self { inner, pos: 0 }
    }
}

impl std::io::Read for ArcVecU8Cursor {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let slice = self.inner.as_ref().as_slice();
        let len = std::cmp::min(slice.len() - self.pos, buf.len());
        buf.put_slice(&slice[self.pos..self.pos + len]);
        self.pos += len;
        Ok(len)
    }
}

impl Cursor<Arc<Vec<u8>>> for ArcVecU8Cursor {
    fn into_inner(self) -> Arc<Vec<u8>> {
        self.inner
    }
}

#[derive(Debug)]
pub struct PrimitiveCursorVoid;

impl std::io::Read for PrimitiveCursorVoid {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Cursor<()> for PrimitiveCursorVoid {
    fn into_inner(self) {}
}

impl StorageKey for () {
    type Cursor = PrimitiveCursorVoid;

    fn weight(&self) -> usize {
        0
    }

    fn serialized_len(&self) -> usize {
        0
    }

    fn read(_buf: &[u8]) -> CodingResult<Self> {
        Ok(())
    }

    fn into_cursor(self) -> Self::Cursor {
        PrimitiveCursorVoid
    }
}

impl StorageValue for () {
    type Cursor = PrimitiveCursorVoid;

    fn weight(&self) -> usize {
        0
    }

    fn serialized_len(&self) -> usize {
        0
    }

    fn read(_buf: &[u8]) -> CodingResult<Self> {
        Ok(())
    }

    fn into_cursor(self) -> Self::Cursor {
        PrimitiveCursorVoid
    }
}

#[derive(Debug)]
pub struct UnimplementedCursor<T: Send + Sync + 'static + std::fmt::Debug>(PhantomData<T>);

impl<T: Send + Sync + 'static + std::fmt::Debug> std::io::Read for UnimplementedCursor<T> {
    fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!()
    }
}

impl<T: Send + Sync + 'static + std::fmt::Debug> Cursor<T> for UnimplementedCursor<T> {
    fn into_inner(self) -> T {
        unimplemented!()
    }
}
