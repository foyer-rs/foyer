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
};

use allocator_api2::alloc::{handle_alloc_error, Allocator, Global, Layout};
use foyer_common::bits;

pub const ALIGN: usize = 4096;

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
        let capacity = bits::align_up(ALIGN, capacity);
        let layout = unsafe { Layout::from_size_align_unchecked(capacity, ALIGN) };
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
    pub fn into_owned_slice(self) -> OwnedIoSlice {
        let end = self.len();
        OwnedIoSlice {
            buffer: self,
            start: 0,
            end,
        }
    }
}

impl Drop for IoBuffer {
    fn drop(&mut self) {
        let layout = unsafe { Layout::from_size_align_unchecked(self.cap, ALIGN) };
        unsafe { Global.deallocate(NonNull::new_unchecked(self.ptr), layout) };
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
        &*self
    }
}

impl AsMut<[u8]> for IoBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

/// A slice on the io buffer with ownership.
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
    pub fn into_buffer(self) -> IoBuffer {
        self.buffer
    }

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

        OwnedIoSlice {
            buffer: self.buffer,
            start: s,
            end: e,
        }
    }

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

        OwnedIoSlice {
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
        &*self
    }
}

impl AsMut<[u8]> for OwnedIoSlice {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

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
        let buf = IoBuffer::new(4096);

        let mut slice = buf.into_owned_slice();
        assert_eq!(slice.len(), 4096);

        (&mut slice[..]).put_bytes(1, 1777);

        let mut slice = slice.slice(1024..);
        assert_eq!(slice.len(), 3072);

        (&mut slice[2048..]).put_bytes(4, 1024);

        let mut slice = slice.slice(..=2047);
        assert_eq!(slice.len(), 2048);

        (&mut slice[1024..]).put_bytes(3, 1024);

        let mut slice = slice.slice(..1024);
        assert_eq!(slice.len(), 1024);

        (&mut slice[..]).put_bytes(2, 1024);

        let buf = slice.into_buffer();
        let expected = [vec![1u8; 1024], vec![2u8; 1024], vec![3u8; 1024], vec![4u8; 1024]].concat();
        assert_eq!(&buf[..], &expected);
    }
}
