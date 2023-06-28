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

#![feature(trait_alias)]

use bytes::{Buf, BufMut};

pub trait Item = Sized + Send + Sync + 'static;

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
    + std::fmt::Debug
{
    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Key` if storage is used.")
    }

    fn write(&self, _buf: &mut [u8]) {
        panic!("Method `write` must be implemented for `Key` if storage is used.")
    }

    fn read(_buf: &[u8]) -> Self {
        panic!("Method `read` must be implemented for `Key` if storage is used.")
    }
}

pub trait Value: Sized + Send + Sync + 'static + std::fmt::Debug {
    fn serialized_len(&self) -> usize {
        panic!("Method `serialized_len` must be implemented for `Value` if storage is used.")
    }

    fn write(&self, _buf: &mut [u8]) {
        panic!("Method `write` must be implemented for `Value` if storage is used.")
    }

    fn read(_buf: &[u8]) -> Self {
        panic!("Method `read` must be implemented for `Value` if storage is used.")
    }
}

// TODO(MrCroxx): use macro to impl for all

impl Key for u64 {
    fn serialized_len(&self) -> usize {
        8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(*self);
    }

    fn read(mut buf: &[u8]) -> Self {
        buf.get_u64()
    }
}

impl Key for u32 {
    fn serialized_len(&self) -> usize {
        4
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(*self);
    }

    fn read(mut buf: &[u8]) -> Self {
        buf.get_u32()
    }
}

impl Value for Vec<u8> {
    fn serialized_len(&self) -> usize {
        self.len()
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_slice(self);
    }

    fn read(buf: &[u8]) -> Self {
        buf.to_vec()
    }
}
