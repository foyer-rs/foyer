//  Copyright 2024 foyer Project Authors
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
    cell::UnsafeCell,
    fmt::Debug,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

struct SyncUnsafeCell<T> {
    inner: UnsafeCell<T>,
}

unsafe impl<T> Sync for SyncUnsafeCell<T> where T: Sync {}

impl<T> Deref for SyncUnsafeCell<T> {
    type Target = UnsafeCell<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for SyncUnsafeCell<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> SyncUnsafeCell<T> {
    fn new(value: T) -> Self {
        Self {
            inner: UnsafeCell::new(value),
        }
    }

    fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

/// Index of the slab.
type RawToken = usize;

/// A token that can be used to access the allocated entry in [`Slab`].
///
/// # Safety
///
/// [`Token`] can be used like an index. It will be stale if the entry is removed from the slab.
///
/// Use a stale token to access the slab will lead to an UB.
#[derive(Debug, Clone, Copy)]
pub struct Token(NonZeroUsize);

impl Token {
    const HIGHEST_BIT: RawToken = 1 << (RawToken::BITS - 1);

    fn from_raw(raw: RawToken) -> Self {
        assert_eq!(raw & Self::HIGHEST_BIT, 0);
        let val = unsafe { NonZeroUsize::new_unchecked(raw | Self::HIGHEST_BIT) };
        Self(val)
    }

    fn into_raw(self) -> RawToken {
        self.0.get() & !Self::HIGHEST_BIT
    }
}

enum Entry<T> {
    Vacant(usize),
    Occupied(SyncUnsafeCell<T>),
}

/// Use [`Vec`] with `with_capacity` to prevent from reallocation and avoid handmade allocation, because a lot of useful
/// functions for allocation and layout calculation are still unstable and for std/compiler usage only.
type Segment<T> = Vec<Entry<T>>;

/// A segmented slab allocator for typed data structures to keep allocation/deallocation ops cheap.
///
/// The allocated memory are guaranteed not to be moved. It is safe to use pointers to reference the allocated memory.
pub struct Slab<T> {
    /// Allocated segments.
    segments: Vec<Segment<T>>,
    /// The count of entries that the segment can hold.
    segment_entries: usize,
    /// Allocated entry count.
    len: usize,
    /// Next slot to allocate.
    next: usize,
}

impl<T> Debug for Slab<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slab")
            .field("capacity", &self.capacity())
            .field("segment_entries", &self.segment_entries)
            .field("len", &self.len)
            .field("next", &self.next)
            .finish()
    }
}

impl<T> Slab<T> {
    /// The count of entries that the slab can hold without further allocation.
    pub fn capacity(&self) -> usize {
        self.segments.len() * self.segment_entries
    }

    /// The count of allocated entry count.
    #[expect(unused)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Insert a new entry into the slab.
    pub fn insert(&mut self, val: T) -> Token {
        let cell = SyncUnsafeCell::new(val);

        let index = self.next;
        let (s, i) = self.pos(index);

        tracing::trace!("insert slab {self:?} at {}", index);

        if s == self.segments.len() {
            // Allocate a new segment.
            self.segments.push(Segment::with_capacity(self.segment_entries));
        }

        if i == self.segments[s].len() {
            self.segments[s].push(Entry::Occupied(cell));
            self.next = index + 1;
        } else {
            self.next = match &self.segments[s][i] {
                &Entry::Vacant(next) => next,
                _ => panic!("invalid index (unexpected occupied): {index}, segment: {s}, entry index: {i}"),
            };
            self.segments[s][i] = Entry::Occupied(cell);
        }

        self.len += 1;

        Token::from_raw(index)
    }

    /// Remove an inserted entry from slab.
    ///
    /// # Safety
    ///
    /// Accessing with a stale token will lead to UB.
    pub fn remove(&mut self, token: Token) -> T {
        let index = token.into_raw();
        let (s, i) = self.pos(index);

        tracing::trace!("remove slab {self:?} at {}", index);

        match std::mem::replace(&mut self.segments[s][i], Entry::Vacant(self.next)) {
            Entry::Occupied(cell) => {
                self.next = index;
                self.len -= 1;
                cell.into_inner()
            }
            _ => panic!("invalid index (unexpected vacant): {index}, segment: {s}, entry index: {i}"),
        }
    }

    /// Get the [`NonNull`] pointer of a entry from slab.
    ///
    /// # Safety
    ///
    /// Accessing with a stale token will lead to UB.
    pub fn ptr(&self, token: Token) -> NonNull<T> {
        let index = token.into_raw();
        let (s, i) = self.pos(index);

        tracing::trace!("access slab {self:?} at {}", index);

        match &self.segments[s][i] {
            Entry::Occupied(cell) => unsafe { NonNull::new_unchecked(cell.get()) },
            _ => panic!("invalid index (unexpected vacant): {index}, segment: {s}, entry index: {i}"),
        }
    }

    fn pos(&self, index: usize) -> (usize, usize) {
        (index / self.segment_entries, index % self.segment_entries)
    }
}

/// Builder for [`Slab`].
pub struct SlabBuilder<T> {
    capacity: usize,
    segment_entries: usize,
    _marker: PhantomData<T>,
}

impl<T> Default for SlabBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SlabBuilder<T> {
    /// Default segment size for [`Slab`].
    pub const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024; // 64 KiB

    pub fn new() -> Self {
        Self {
            capacity: 0,
            segment_entries: Self::segment_entries(Self::DEFAULT_SEGMENT_SIZE),
            _marker: PhantomData,
        }
    }

    /// Set the capacity of [`Slab`].
    ///
    /// The capacity is the count of entries that the slab can hold without further allocation.
    pub fn with_capacity(mut self, val: usize) -> Self {
        self.capacity = val;
        self
    }

    /// Set the capacity of the segment of [`Slab`] by entries.
    ///
    /// The capacity is the count of entries that the segment can hold without further allocation.
    #[expect(unused)]
    pub fn with_segment_entries(mut self, val: usize) -> Self {
        self.segment_entries = val;
        self
    }

    /// Set the capacity of the segment of [`Slab`] by size.
    ///
    /// The capacity is the count of entries that the segment can hold without further allocation.
    pub fn with_segment_size(mut self, val: usize) -> Self {
        self.segment_entries = Self::segment_entries(val);
        self
    }

    /// Build the [`Slab`] with the given configuration.
    pub fn build(self) -> Slab<T> {
        let segment_count = self.capacity / self.segment_entries
            + match self.capacity / self.segment_entries {
                0 => 0,
                _ => 1,
            };
        Slab {
            segments: (0..segment_count)
                .map(|_| Segment::with_capacity(self.segment_entries))
                .collect(),
            segment_entries: self.segment_entries,
            len: 0,
            next: 0,
        }
    }

    fn segment_entries(segment_size: usize) -> usize {
        segment_size / std::mem::size_of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Data {
        a: u64,
        b: Vec<u8>,
    }

    #[test]
    fn test_slab_basic() {
        let mut slab = SlabBuilder::new().build();

        let t1 = slab.insert(Data { a: 1, b: vec![1; 1024] });
        let t2 = slab.insert(Data { a: 2, b: vec![2; 1024] });

        unsafe { slab.ptr(t1).as_mut() }.a = 2;
        unsafe { slab.ptr(t2).as_mut() }.a = 1;

        let d1 = slab.remove(t1);
        let d2 = slab.remove(t2);

        assert_eq!(d1, Data { a: 2, b: vec![1; 1024] });
        assert_eq!(d2, Data { a: 1, b: vec![2; 1024] });
    }

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<Slab<()>>();
    }
}
