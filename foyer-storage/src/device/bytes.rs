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

use std::{
    fmt::Debug,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::Arc,
};

use super::{allocator::AlignedAllocator, ALIGN, IO_BUFFER_ALLOCATOR};
use allocator_api2::vec::Vec as VecA;
use bytes::{buf::UninitSlice, Buf, BufMut};
use foyer_common::bits;

/// A 4K-aligend u8 vector.
pub struct IoBytesMut {
    inner: VecA<u8, &'static AlignedAllocator<ALIGN>>,
}

impl Debug for IoBytesMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&*self.inner, f)
    }
}

impl From<VecA<u8, &'static AlignedAllocator<ALIGN>>> for IoBytesMut {
    fn from(value: VecA<u8, &'static AlignedAllocator<ALIGN>>) -> Self {
        Self { inner: value }
    }
}

impl Default for IoBytesMut {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for IoBytesMut {
    type Target = VecA<u8, &'static AlignedAllocator<ALIGN>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for IoBytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl PartialEq for IoBytesMut {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for IoBytesMut {}

impl Clone for IoBytesMut {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Ported from `bytes`.
unsafe impl BufMut for IoBytesMut {
    #[inline]
    fn remaining_mut(&self) -> usize {
        // A vector can never have more than isize::MAX bytes
        isize::MAX as usize - self.len()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        let len = self.len();
        let remaining = self.capacity() - len;

        if remaining < cnt {
            panic_advance(cnt, remaining);
        }

        // Addition will not overflow since the sum is at most the capacity.
        self.set_len(len + cnt);
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        if self.capacity() == self.len() {
            self.reserve(64); // Grow the vec
        }

        let cap = self.capacity();
        let len = self.len();

        let ptr = self.as_mut_ptr();
        // SAFETY: Since `ptr` is valid for `cap` bytes, `ptr.add(len)` must be
        // valid for `cap - len` bytes. The subtraction will not underflow since
        // `len <= cap`.
        unsafe { UninitSlice::from_raw_parts_mut(ptr.add(len), cap - len) }
    }

    // Specialize these methods so they can skip checking `remaining_mut`
    // and `advance_mut`.
    #[inline]
    fn put<T: Buf>(&mut self, mut src: T)
    where
        Self: Sized,
    {
        // In case the src isn't contiguous, reserve upfront.
        self.reserve(src.remaining());

        while src.has_remaining() {
            let s = src.chunk();
            let l = s.len();
            self.extend_from_slice(s);
            src.advance(l);
        }
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }

    #[inline]
    fn put_bytes(&mut self, val: u8, cnt: usize) {
        // If the addition overflows, then the `resize` will fail.
        let new_len = self.len().saturating_add(cnt);
        self.resize(new_len, val);
    }
}

/// Panic with a nice error message.
///
/// Ported from `bytes`.
#[cold]
fn panic_advance(idx: usize, len: usize) -> ! {
    panic!("advance out of bounds: the len is {} but advancing by {}", len, idx);
}

impl IoBytesMut {
    /// Constructs a new, empty 4K-aligned u8 vector.
    pub fn new() -> Self {
        Self {
            inner: VecA::new_in(&IO_BUFFER_ALLOCATOR),
        }
    }

    /// Constructs a new, empty , empty 4K-aligned u8 vector
    /// with at least the specified capacity with the provided allocator.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecA::with_capacity_in(capacity, &IO_BUFFER_ALLOCATOR),
        }
    }

    // Splits the collection into two at the given index.
    ///
    /// Returns a newly allocated vector containing the elements in the range
    /// `[at, len)`. After the call, the original vector will be left containing
    /// the elements `[0, at)` with its previous capacity unchanged.
    ///
    /// # Panics
    ///
    /// The split point `at` must be 4K-aligned.
    pub fn split_off(&mut self, at: usize) -> Self {
        debug_assert_eq!(at % ALIGN, 0);
        let inner = self.inner.split_off(at);
        Self { inner }
    }

    /// Convert [`IoBytesMut`] to [`IoBytes`].
    pub fn freeze(self) -> IoBytes {
        self.into()
    }
}

/// A 4K-aligned, shared, immutable u8 vector.
pub struct IoBytes {
    inner: Arc<VecA<u8, &'static AlignedAllocator<ALIGN>>>,
    offset: usize,
    len: usize,
}

impl Debug for IoBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&*self.inner, f)
    }
}

impl From<VecA<u8, &'static AlignedAllocator<ALIGN>>> for IoBytes {
    fn from(mut value: VecA<u8, &'static AlignedAllocator<ALIGN>>) -> Self {
        let offset = 0;
        let len = value.len();

        let aligned = bits::align_up(ALIGN, value.len());
        value.reserve_exact(aligned - value.len());
        unsafe { value.set_len(aligned) };

        let inner = Arc::new(value);

        Self { inner, offset, len }
    }
}

impl From<IoBytesMut> for IoBytes {
    fn from(value: IoBytesMut) -> Self {
        value.inner.into()
    }
}

impl Default for IoBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for IoBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner[self.offset..self.offset + self.len]
    }
}

impl PartialEq for IoBytes {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for IoBytes {}

impl Clone for IoBytes {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl IoBytes {
    /// Constructs a new, empty, shared 4K-aligned u8 vector.
    pub fn new() -> Self {
        IoBytesMut::new().into()
    }

    /// Returns a 4K-aligned slice of self for the provided range.
    ///
    /// # Panics
    ///
    /// - The range must be valid.
    /// - The new slice must still be 4K-aligned.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(i) => self.offset + *i,
            Bound::Excluded(i) => self.offset + *i + 1,
            Bound::Unbounded => self.offset,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => self.offset + *i + 1,
            Bound::Excluded(i) => self.offset + *i,
            Bound::Unbounded => self.offset + self.len,
        };
        assert_eq!(start % ALIGN, 0);
        if start > end || start < self.offset || end > self.offset + self.len {
            panic!("slice index out of bound");
        }
        Self {
            inner: self.inner.clone(),
            offset: start,
            len: end - start,
        }
    }

    /// As 4K-aligned u8 slice.
    ///
    /// For the underlying vector has reserved aligned size when creating,
    /// this operation is always safe.
    pub fn as_aligned(&self) -> &[u8] {
        let start = self.offset;
        let end = bits::align_up(ALIGN, self.offset + self.len);
        debug_assert_eq!(start % ALIGN, 0);
        debug_assert_eq!(end % ALIGN, 0);
        &self.inner[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_bytes() {
        let mut buf = IoBytesMut::new();
        buf.put_slice(&[42; 1024]);
        assert_eq!(buf.as_ptr() as usize % ALIGN, 0);

        let buf = buf.freeze();
        assert_eq!(buf.len(), 1024);
        assert_eq!(buf.as_aligned().len(), ALIGN);
        let buf = buf.slice(..42);
        assert_eq!(buf.len(), 42);
        assert_eq!(buf.as_aligned().len(), ALIGN);
    }
}
