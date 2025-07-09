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

/// 4K-aligned immutable buf for direct I/O.
pub trait IoBuf: Deref<Target = [u8]> + Send + Sync + 'static {}

/// 4K-aligned mutable buf for direct I/O.
pub trait IoBufMut: DerefMut<Target = [u8]> + Send + Sync + 'static {}

use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use allocator_api2::alloc::{handle_alloc_error, Allocator, Global, Layout};
use foyer_common::bits;

use super::PAGE;

/// 4K-aligned raw bytes.
#[derive(Debug)]
pub struct Raw {
    ptr: *mut u8,
    cap: usize,
}

unsafe impl Send for Raw {}
unsafe impl Sync for Raw {}

impl Raw {
    /// Allocate an 4K-aligned [`Raw`] with **AT LEAST** `capacity` bytes.
    pub fn new(capacity: usize) -> Self {
        let capacity = bits::align_up(PAGE, capacity);
        let layout = unsafe { Layout::from_size_align_unchecked(capacity, PAGE) };
        let mut nonnull = match Global.allocate(layout) {
            Ok(nonnull) => nonnull,
            Err(_) => handle_alloc_error(layout),
        };
        let slice = unsafe { nonnull.as_mut() };
        let ptr = slice.as_mut_ptr();
        let cap = slice.len();
        Self { ptr, cap }
    }

    /// Consume [`Raw`] and get the raw pointer and the capacity.
    ///
    /// # Safety
    ///
    /// [`Raw::from_raw_parts`] must be called later. Otherwise the buffer memory will leak.
    pub fn into_raw_parts(self) -> (*mut u8, usize) {
        let res = (self.ptr, self.cap);
        std::mem::forget(self);
        res
    }

    /// Construct [`Raw`] with the raw pointer and the capacity.
    ///
    /// # Safety
    ///
    /// The `ptr` and `cap` must be returned by [`Raw::into_raw_parts`].
    pub unsafe fn from_raw_parts(ptr: *mut u8, cap: usize) -> Self {
        Self { ptr, cap }
    }
}

impl Clone for Raw {
    fn clone(&self) -> Self {
        let mut buf = Raw::new(self.cap);
        assert_eq!(buf.cap, self.cap);
        buf.copy_from_slice(self);
        buf
    }
}

impl Drop for Raw {
    fn drop(&mut self) {
        let layout = unsafe { Layout::from_size_align_unchecked(self.cap, PAGE) };
        unsafe { Global.deallocate(NonNull::new_unchecked(self.ptr), layout) };
    }
}

impl Deref for Raw {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { from_raw_parts(self.ptr, self.cap) }
    }
}

impl DerefMut for Raw {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl AsRef<[u8]> for Raw {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for Raw {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

impl PartialEq for Raw {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Raw {}

impl IoBuf for Raw {}
impl IoBufMut for Raw {}

pub struct IoBytes {
    raw: Arc<Raw>,
    start: usize,
    end: usize,
}

impl From<Raw> for IoBytes {
    fn from(value: Raw) -> Self {
        let len = value.len();
        Self {
            raw: Arc::new(value),
            start: 0,
            end: len,
        }
    }
}

impl From<IoBytesMut> for IoBytes {
    fn from(value: IoBytesMut) -> Self {
        let len = value.len();
        Self {
            raw: Arc::new(value.raw),
            start: 0,
            end: len,
        }
    }
}

impl Deref for IoBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.raw[self.start..self.end]
    }
}

impl AsRef<[u8]> for IoBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

#[derive(Debug, Clone)]
pub struct IoBytesMut {
    raw: Raw,
    len: usize,
}

impl IoBytesMut {
    pub fn capacity(&self) -> usize {
        self.raw.cap
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Deref for IoBytesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.raw[..self.len]
    }
}

impl DerefMut for IoBytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw[..self.len]
    }
}

impl AsRef<[u8]> for IoBytesMut {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for IoBytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.raw[..self.len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dyn() {
        let raw = Raw::new(4096);
        let _: Box<dyn IoBuf> = Box::new(raw.clone());
        let _: Box<dyn IoBufMut> = Box::new(raw.clone());
    }
}
