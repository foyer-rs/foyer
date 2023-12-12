//  Copyright 2023 MrCroxx
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

use std::marker::PhantomData;

use bytes::{Buf, BufMut};
use paste::paste;

pub type CodingError = anyhow::Error;
pub type CodingResult<T> = Result<T, CodingError>;

trait BufMutExt: BufMut {
    cfg_match! {
        cfg(target_pointer_width = "16") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u16(v as u16);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i16(v as i16);
            }
        }
        cfg(target_pointer_width = "32") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u32(v as u32);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i32(v as i32);
            }
        }
        cfg(target_pointer_width = "64") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u64(v as u64);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i64(v as i64);
            }
        }
    }
}

impl<T: Buf> BufExt for T {}

trait BufExt: Buf {
    cfg_match! {
        cfg(target_pointer_width = "16") => {
            fn get_usize(&mut self) -> usize {
                self.get_u16() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i16() as isize
            }
        }
        cfg(target_pointer_width = "32") => {
            fn get_usize(&mut self) -> usize {
                self.get_u32() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i32() as isize
            }
        }
        cfg(target_pointer_width = "64") => {
            fn get_usize(&mut self) -> usize {
                self.get_u64() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i64() as isize
            }
        }
    }
}

impl<T: BufMut> BufMutExt for T {}

pub trait Cursor: Send + Sync + 'static + std::io::Read {
    type T: Send + Sync + 'static;

    fn inner(&self) -> &Self::T;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[expect(unused_variables)]
pub trait Key:
    Sized
    + Send
    + Sync
    + 'static
    + std::hash::Hash
    + Eq
    + PartialEq
    + Ord
    + PartialOrd
    + Clone
    + std::fmt::Debug
{
    type Cursor: Cursor<T = Self> = UnimplementedCursor<Self>;

    /// memory weight
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Key` if storage is used.")
    }

    fn write(&self, buf: &mut [u8]) -> CodingResult<()> {
        panic!("Method `write` must be implemented for `Key` if storage is used.")
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        panic!("Method `read` must be implemented for `Key` if storage is used.")
    }

    fn into_cursor(self) -> Self::Cursor {
        panic!("Associated type `Cursor` and method `into_cursor` must be implemented for `Key` if storage is used.")
    }
}

#[expect(unused_variables)]
pub trait Value: Sized + Send + Sync + 'static + std::fmt::Debug {
    type Cursor: Cursor<T = Self> = UnimplementedCursor<Self>;

    /// memory weight
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Value` if storage is used.")
    }

    fn write(&self, buf: &mut [u8]) -> CodingResult<()> {
        panic!("Method `write` must be implemented for `Value` if storage is used.")
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
                pub struct [<PrimitiveCursor $id>] {
                    inner: $type,
                    pos: u8,
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

                impl Cursor for [<PrimitiveCursor $id>] {
                    type T = $type;

                    fn inner(&self) -> &Self::T {
                        &self.inner
                    }

                    fn len(&self) -> usize {
                        std::mem::size_of::<$type>()
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
                impl Key for $type {
                    type Cursor = [<PrimitiveCursor $id>];

                    fn serialized_len(&self) -> usize {
                        std::mem::size_of::<$type>()
                    }


                    fn write(&self, mut buf: &mut [u8]) -> CodingResult<()> {
                        buf.[< put_ $type>](*self);
                        Ok(())
                    }


                    fn read(mut buf: &[u8]) -> CodingResult<Self> {
                        Ok(buf.[< get_ $type>]())
                    }

                    fn into_cursor(self) -> Self::Cursor {
                        [<PrimitiveCursor $id>] {
                            inner: self,
                            pos: 0,
                        }
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
                impl Value for $type {
                    type Cursor = [<PrimitiveCursor $id>];

                    fn serialized_len(&self) -> usize {
                        std::mem::size_of::<$type>()
                    }


                    fn write(&self, mut buf: &mut [u8]) -> CodingResult<()> {
                        buf.[< put_ $type>](*self);
                        Ok(())
                    }


                    fn read(mut buf: &[u8]) -> CodingResult<Self> {
                        Ok(buf.[< get_ $type>]())
                    }

                    fn into_cursor(self) -> Self::Cursor {
                        [<PrimitiveCursor $id>] {
                            inner: self,
                            pos: 0,
                        }
                    }
                }
            )*
        }
    };
}

for_all_primitives! { def_cursor }
for_all_primitives! { impl_key }
for_all_primitives! { impl_value }

impl Key for Vec<u8> {
    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn write(&self, mut buf: &mut [u8]) -> CodingResult<()> {
        buf.put_slice(self);
        Ok(())
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(buf.to_vec())
    }
}

impl Value for Vec<u8> {
    type Cursor = std::io::Cursor<Vec<u8>>;

    fn weight(&self) -> usize {
        self.len()
    }

    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn write(&self, mut buf: &mut [u8]) -> CodingResult<()> {
        buf.put_slice(self);
        Ok(())
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        Ok(buf.to_vec())
    }

    fn into_cursor(self) -> Self::Cursor {
        std::io::Cursor::new(self)
    }
}

impl Cursor for std::io::Cursor<Vec<u8>> {
    type T = Vec<u8>;

    fn inner(&self) -> &Self::T {
        self.get_ref()
    }

    fn len(&self) -> usize {
        self.get_ref().len()
    }
}

pub struct PrimitiveCursorVoid;

impl std::io::Read for PrimitiveCursorVoid {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Cursor for PrimitiveCursorVoid {
    type T = ();

    fn inner(&self) -> &Self::T {
        &()
    }

    fn len(&self) -> usize {
        0
    }
}

impl Key for () {
    fn weight(&self) -> usize {
        0
    }

    fn serialized_len(&self) -> usize {
        0
    }

    fn write(&self, _buf: &mut [u8]) -> CodingResult<()> {
        Ok(())
    }

    fn read(_buf: &[u8]) -> CodingResult<Self> {
        Ok(())
    }
}

impl Value for () {
    type Cursor = PrimitiveCursorVoid;

    fn weight(&self) -> usize {
        0
    }

    fn serialized_len(&self) -> usize {
        0
    }

    fn write(&self, _buf: &mut [u8]) -> CodingResult<()> {
        Ok(())
    }

    fn read(_buf: &[u8]) -> CodingResult<Self> {
        Ok(())
    }

    fn into_cursor(self) -> Self::Cursor {
        PrimitiveCursorVoid
    }
}

pub struct UnimplementedCursor<T: Send + Sync + 'static>(PhantomData<T>);

impl<T: Send + Sync + 'static> std::io::Read for UnimplementedCursor<T> {
    fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!()
    }
}

impl<T: Send + Sync + 'static> Cursor for UnimplementedCursor<T> {
    type T = T;

    fn inner(&self) -> &Self::T {
        unimplemented!()
    }

    fn len(&self) -> usize {
        unimplemented!()
    }
}
