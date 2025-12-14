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
    any::Any,
    fmt::Debug,
    ops::{Deref, DerefMut, RangeBounds},
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use allocator_api2::alloc::{handle_alloc_error, Allocator, Global, Layout};
use foyer_common::bits;

use super::PAGE;

pub trait IoB: Deref<Target = [u8]> + Send + Sync + 'static + Debug + Any {
    fn as_raw_parts(&self) -> (*mut u8, usize);

    // TODO(MrCroxx): Remove this after bump MSRV to 1.86.0+
    // https://blog.rust-lang.org/2025/04/03/Rust-1.86.0/
    fn as_any(&self) -> &dyn Any;

    // TODO(MrCroxx): Remove this after bump MSRV to 1.86.0+
    // https://blog.rust-lang.org/2025/04/03/Rust-1.86.0/
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// 4K-aligned immutable buf for direct I/O.
pub trait IoBuf: Deref<Target = [u8]> + IoB {
    // TODO(MrCroxx): Remove this after bump MSRV to 1.86.0+
    // https://blog.rust-lang.org/2025/04/03/Rust-1.86.0/
    fn into_iob(self: Box<Self>) -> Box<dyn IoB>;
}

/// 4K-aligned mutable buf for direct I/O.
pub trait IoBufMut: DerefMut<Target = [u8]> + IoB {
    // TODO(MrCroxx): Remove this after bump MSRV to 1.86.0+
    // https://blog.rust-lang.org/2025/04/03/Rust-1.86.0/
    fn into_iob(self: Box<Self>) -> Box<dyn IoB>;
}

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
    ///
    /// # Safety
    ///
    /// The returned buffer contains uninitialized memory. Callers must initialize
    /// the memory before reading from it to avoid undefined behavior.
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

impl IoB for Raw {
    fn as_raw_parts(&self) -> (*mut u8, usize) {
        (self.ptr, self.cap)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IoBuf for Raw {
    fn into_iob(self: Box<Self>) -> Box<dyn IoB> {
        self
    }
}

impl IoBufMut for Raw {
    fn into_iob(self: Box<Self>) -> Box<dyn IoB> {
        self
    }
}

/// A 4K-aligned slice on the io buffer that can be shared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoSlice {
    raw: Arc<Raw>,
    start: usize,
    end: usize,
}

impl From<Raw> for IoSlice {
    fn from(value: Raw) -> Self {
        let len = value.len();
        Self {
            raw: Arc::new(value),
            start: 0,
            end: len,
        }
    }
}

impl From<IoSliceMut> for IoSlice {
    fn from(value: IoSliceMut) -> Self {
        let len = value.len();
        Self {
            raw: Arc::new(value.raw),
            start: 0,
            end: len,
        }
    }
}

impl Deref for IoSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.raw[self.start..self.end]
    }
}

impl AsRef<[u8]> for IoSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl IoB for IoSlice {
    fn as_raw_parts(&self) -> (*mut u8, usize) {
        let ptr = unsafe { self.raw.ptr.add(self.start) };
        let len = self.end - self.start;
        (ptr, len)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl IoBuf for IoSlice {
    fn into_iob(self: Box<Self>) -> Box<dyn IoB> {
        self
    }
}

impl IoSlice {
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let s = match range.start_bound() {
            std::ops::Bound::Included(i) => self.start + *i,
            std::ops::Bound::Excluded(_) => unreachable!(),
            std::ops::Bound::Unbounded => self.start,
        };

        let e = match range.end_bound() {
            std::ops::Bound::Included(i) => self.start + *i + 1,
            std::ops::Bound::Excluded(i) => self.start + *i,
            std::ops::Bound::Unbounded => self.end,
        };

        if s > e {
            panic!("slice index starts at {s} but ends at {e}");
        }

        bits::assert_aligned(PAGE, s);
        bits::assert_aligned(PAGE, e);

        Self {
            raw: self.raw.clone(),
            start: s,
            end: e,
        }
    }

    /// Convert into [`IoSliceMut`], if the [`IoSlice`] has exactly one reference.
    pub fn try_into_io_slice_mut(self) -> Option<IoSliceMut> {
        let raw = Arc::into_inner(self.raw)?;
        Some(IoSliceMut { raw })
    }
}

/// A 4K-aligned mutable slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoSliceMut {
    raw: Raw,
}

impl IoSliceMut {
    pub fn new(capacity: usize) -> Self {
        let raw = Raw::new(capacity);
        Self { raw }
    }

    pub fn len(&self) -> usize {
        self.raw.cap
    }
}

impl Deref for IoSliceMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl DerefMut for IoSliceMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

impl AsRef<[u8]> for IoSliceMut {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for IoSliceMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.raw
    }
}

impl IoB for IoSliceMut {
    fn as_raw_parts(&self) -> (*mut u8, usize) {
        (self.raw.ptr, self.raw.cap)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
impl IoBuf for IoSliceMut {
    fn into_iob(self: Box<Self>) -> Box<dyn IoB> {
        self
    }
}

impl IoBufMut for IoSliceMut {
    fn into_iob(self: Box<Self>) -> Box<dyn IoB> {
        self
    }
}

impl IoSliceMut {
    pub fn into_io_slice(self) -> IoSlice {
        let start = 0;
        let end = self.raw.cap;
        let raw = Arc::new(self.raw);
        IoSlice { raw, start, end }
    }
}

impl dyn IoB {
    /// Convert into concrete type [`Box<IoSlice>`] if the underlying type fits.
    pub fn try_into_io_slice(self: Box<Self>) -> Option<Box<IoSlice>> {
        let any: Box<dyn Any> = self.into_any();
        any.downcast::<IoSlice>().ok()
    }

    /// Convert into concrete type [`Box<IoSliceMut>`] if the underlying type fits.
    pub fn try_into_io_slice_mut(self: Box<Self>) -> Option<Box<IoSliceMut>> {
        let any: Box<dyn Any> = self.into_any();
        any.downcast::<IoSliceMut>().ok()
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
