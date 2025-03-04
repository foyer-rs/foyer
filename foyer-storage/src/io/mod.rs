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
    ops::{Deref, DerefMut, Range, RangeBounds},
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use allocator_api2::alloc::{handle_alloc_error, Allocator, Global, Layout};
use foyer_common::bits;

pub const PAGE: usize = 4096;

/// 4K-aligned immutable buf for direct I/O.
pub trait IoBuf: Deref<Target = [u8]> + Send + Sync + 'static {}

/// 4K-aligned mutable buf for direct I/O.
pub trait IoBufMut: DerefMut<Target = [u8]> + Send + Sync + 'static {}

/// 4K-aligned capacity-fixed buffer.
#[derive(Debug)]
pub struct IoBuffer {
    ptr: *mut u8,
    cap: usize,
}

unsafe impl Send for IoBuffer {}
unsafe impl Sync for IoBuffer {}

impl IoBuffer {
    /// Allocate an 4K-aligned [`IoBuffer`] with at least `capacity` bytes.
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

    /// Consume [`IoBuffer`] and get the raw pointer and the capacity.
    ///
    /// # Safety
    ///
    /// [`IoBuffer::from_raw_parts`] must be called later. Otherwise the buffer memory will leak.
    pub fn into_raw_parts(self) -> (*mut u8, usize) {
        let res = (self.ptr, self.cap);
        std::mem::forget(self);
        res
    }

    /// Construct [`IoBuffer`] with the raw pointer and the capacity.
    ///
    /// # Safety
    ///
    /// The `ptr` and `cap` must be returned by [`IoBuffer::into_raw_parts`].
    pub unsafe fn from_raw_parts(ptr: *mut u8, cap: usize) -> Self {
        Self { ptr, cap }
    }

    /// Convert into [`OwnedIoSlice`].
    pub fn into_owned_io_slice(self) -> OwnedIoSlice {
        let end = self.len();
        OwnedIoSlice {
            buffer: self,
            start: 0,
            end,
        }
    }

    /// Convert into [`SharedIoSlice`].
    pub fn into_shared_io_slice(self) -> SharedIoSlice {
        let end = self.len();
        SharedIoSlice {
            buffer: Arc::new(self),
            start: 0,
            end,
        }
    }

    /// Convert into [`OwnedSlice`].
    pub fn into_owned_slice(self) -> OwnedSlice {
        let end = self.len();
        OwnedSlice {
            buffer: self,
            start: 0,
            end,
        }
    }
}

impl Drop for IoBuffer {
    fn drop(&mut self) {
        let layout = unsafe { Layout::from_size_align_unchecked(self.cap, PAGE) };
        unsafe { Global.deallocate(NonNull::new_unchecked(self.ptr), layout) };
    }
}

impl Clone for IoBuffer {
    fn clone(&self) -> Self {
        let mut buf = IoBuffer::new(self.cap);
        assert_eq!(buf.len(), self.cap);
        buf.copy_from_slice(self);
        buf
    }
}

impl Deref for IoBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { from_raw_parts(self.ptr, self.cap) }
    }
}

impl DerefMut for IoBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl AsRef<[u8]> for IoBuffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for IoBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

impl IoBuf for IoBuffer {}
impl IoBufMut for IoBuffer {}

/// A 4K-aligned slice on the io buffer with ownership.
///
/// [`OwnedIoSlice`] can convert from or convert to an [`IoBuffer`].
#[derive(Debug)]
pub struct OwnedIoSlice {
    buffer: IoBuffer,
    start: usize,
    end: usize,
}

impl OwnedIoSlice {
    /// Convert into [`IoBuffer`].
    pub fn into_io_buffer(self) -> IoBuffer {
        self.buffer
    }

    /// Slice the [`OwnedIoSlice`] with relative range.
    ///
    /// # Safety
    ///
    /// The range start and end must be 4K-aligned.
    pub fn slice(self, range: impl RangeBounds<usize> + std::fmt::Debug) -> Self {
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
            buffer: self.buffer,
            start: s,
            end: e,
        }
    }

    /// Slice the [`OwnedIoSlice`] with absolute range.
    ///
    /// # Safety
    ///
    /// The range start and end must be 4K-aligned.
    pub fn absolute_slice(self, range: impl RangeBounds<usize>) -> Self {
        let s = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(_) => unreachable!(),
            std::ops::Bound::Unbounded => 0,
        };

        let e = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.buffer.len(),
        };

        if s > e {
            panic!("slice index starts at {s} but ends at {e}");
        }

        bits::assert_aligned(PAGE, s);
        bits::assert_aligned(PAGE, e);

        Self {
            buffer: self.buffer,
            start: s,
            end: e,
        }
    }

    /// Get the absolute range of the [`OwnedIoSlice`] over a [`IoBuffer`].
    pub fn absolute(&self) -> Range<usize> {
        self.start..self.end
    }
}

impl Deref for OwnedIoSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[self.start..self.end]
    }
}

impl DerefMut for OwnedIoSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[self.start..self.end]
    }
}

impl AsRef<[u8]> for OwnedIoSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for OwnedIoSlice {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

impl IoBuf for OwnedIoSlice {}
impl IoBufMut for OwnedIoSlice {}

/// A 4K-aligned slice on the io buffer that can be shared.
#[derive(Debug, Clone)]
pub struct SharedIoSlice {
    buffer: Arc<IoBuffer>,
    start: usize,
    end: usize,
}

impl SharedIoSlice {
    /// Slice the [`SharedIoSlice`] with relative range.
    ///
    /// # Safety
    ///
    /// The range start and end must be 4K-aligned.
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
            buffer: self.buffer.clone(),
            start: s,
            end: e,
        }
    }

    /// Slice the [`SharedIoSlice`] with absolute range.
    ///
    /// # Safety
    ///
    /// The range start and end must be 4K-aligned.
    pub fn absolute_slice(&self, range: impl RangeBounds<usize>) -> Self {
        let s = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(_) => unreachable!(),
            std::ops::Bound::Unbounded => 0,
        };

        let e = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.buffer.len(),
        };

        if s > e {
            panic!("slice index starts at {s} but ends at {e}");
        }

        bits::assert_aligned(PAGE, s);
        bits::assert_aligned(PAGE, e);

        Self {
            buffer: self.buffer.clone(),
            start: s,
            end: e,
        }
    }

    /// Get the absolute range of the [`OwnedIoSlice`] over a [`IoBuffer`].
    pub fn absolute(&self) -> Range<usize> {
        self.start..self.end
    }
}

impl Deref for SharedIoSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[self.start..self.end]
    }
}

impl AsRef<[u8]> for SharedIoSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl IoBuf for SharedIoSlice {}

/// A slice on the io buffer with ownership.
///
/// [`OwnedSlice`] can convert from or convert to an [`IoBuffer`].
#[derive(Debug)]
pub struct OwnedSlice {
    buffer: IoBuffer,
    start: usize,
    end: usize,
}

impl OwnedSlice {
    /// Convert into [`IoBuffer`].
    pub fn into_io_buffer(self) -> IoBuffer {
        self.buffer
    }

    /// Slice the [`OwnedIoSlice`] with relative range.
    pub fn slice(self, range: impl RangeBounds<usize>) -> Self {
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

        Self {
            buffer: self.buffer,
            start: s,
            end: e,
        }
    }

    /// Slice the [`OwnedIoSlice`] with absolute range.
    pub fn absolute_slice(self, range: impl RangeBounds<usize>) -> Self {
        let s = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(_) => unreachable!(),
            std::ops::Bound::Unbounded => 0,
        };

        let e = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.buffer.len(),
        };

        if s > e {
            panic!("slice index starts at {s} but ends at {e}");
        }

        Self {
            buffer: self.buffer,
            start: s,
            end: e,
        }
    }

    /// Get the absolute range of the [`OwnedIoSlice`] over a [`IoBuffer`].
    pub fn absolute(&self) -> Range<usize> {
        self.start..self.end
    }
}

impl Deref for OwnedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[self.start..self.end]
    }
}

impl DerefMut for OwnedSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[self.start..self.end]
    }
}

impl AsRef<[u8]> for OwnedSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for OwnedSlice {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_io_buffer_alloc_dealloc() {
        let buf = IoBuffer::new(100);
        drop(buf);

        let buf = IoBuffer::new(4096);
        drop(buf);

        let buf = IoBuffer::new(4097);
        drop(buf);

        let buf = IoBuffer::new(60000);
        let (ptr, cap) = buf.into_raw_parts();
        let buf = unsafe { IoBuffer::from_raw_parts(ptr, cap) };
        drop(buf);
    }

    #[test]
    fn test_owned_io_slice() {
        let buf = IoBuffer::new(PAGE * 3);

        let slice = buf.into_owned_io_slice();
        assert_eq!(slice.len(), PAGE * 3);

        let mut s23 = slice.slice(PAGE..);
        s23.fill(45);

        let mut s3 = s23.slice(PAGE..);
        s3.fill(14);

        let mut s1 = s3.absolute_slice(..PAGE);
        s1.fill(11);

        let buf = s1.into_io_buffer();

        let expected = [vec![11u8; PAGE], vec![45u8; PAGE], vec![14u8; PAGE]].concat();
        assert_eq!(&buf[..], &expected);
    }

    #[test]
    fn test_shared_io_slice() {
        let mut buf = IoBuffer::new(3 * PAGE);
        buf.fill(11);
        buf[PAGE..].fill(45);
        buf[PAGE * 2..].fill(14);

        let buf = buf.into_shared_io_slice();
        let s1 = buf.slice(0..PAGE);
        let s2 = buf.slice(PAGE..PAGE * 2);
        let s3 = buf.slice(PAGE * 2..PAGE * 3);

        assert_eq!(&s1[..], &[11u8; PAGE]);
        assert_eq!(&s2[..], &[45u8; PAGE]);
        assert_eq!(&s3[..], &[14u8; PAGE]);
    }
}
