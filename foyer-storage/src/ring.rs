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

use std::{
    alloc::Global,
    collections::BTreeSet,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use foyer_common::code::{Key, Value};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::device::BufferAllocator;

pub struct Buffer<K, V, A = Global>
where
    K: Key,
    V: Value,
    A: BufferAllocator,
{
    queue: Arc<Mutex<BTreeSet<SequenceOrderedItem<K, A>>>>,
    index: Arc<Mutex<BTreeSet<KeyOrderedItem<K, A>>>>,

    ring: Arc<RingBuffer<A>>,

    _marker: PhantomData<V>,
}

impl<K, V, A> Buffer<K, V, A>
where
    K: Key,
    V: Value,
    A: BufferAllocator,
{
    pub async fn insert(&self, key: K, value: V) {
        let mut view = loop {
            if let Some(view) = self.ring.allocate(value.serialized_len()).await {
                break view;
            }
        };

        value.write(&mut view);

        todo!()
    }
}

struct Item<K, A = Global>
where
    K: Key,
    A: BufferAllocator,
{
    sequence: usize,
    key: K,
    view: ViewMut<A>,
}

struct SequenceOrderedItem<K, A = Global>
where
    K: Key,
    A: BufferAllocator,
{
    item: Arc<Item<K, A>>,
}

struct KeyOrderedItem<K, A = Global>
where
    K: Key,
    A: BufferAllocator,
{
    item: Arc<Item<K, A>>,
}

#[derive(Debug)]
pub struct RingBuffer<A = Global>
where
    A: BufferAllocator,
{
    data: Vec<u8, A>,
    capacity: usize,

    allocated: AtomicUsize,
    released: AtomicUsize,

    nofify: Notify,
}

impl<A> RingBuffer<A>
where
    A: BufferAllocator,
{
    /// Allocate a mutable view with required size on the ring buffer.
    ///
    /// Returns `None` when the allocated buffer cross the boundary.
    pub async fn allocate(self: &Arc<Self>, len: usize) -> Option<ViewMut<A>> {
        let offset = self.allocated.fetch_add(len, Ordering::SeqCst);

        if offset / self.capacity != (offset + len) / self.capacity {
            return None;
        }
        let offset = offset % self.capacity;

        let view = Some(ViewMut::new(self, offset, len));

        loop {
            if self.released.load(Ordering::Acquire) >= offset + len {
                break view;
            }
            self.nofify.notified().await;
        }
    }

    /// Release buffer to `position`.
    ///
    /// The buffer between last `released` point and `position` will be released after
    /// all views on it destroyed.
    pub fn release(&self, position: usize) {
        todo!()
    }
}

/// # Safety
///
/// The underlying buffer of [`ViewMut`] must be valid during its lifetime.
pub struct ViewMut<A = Global>
where
    A: BufferAllocator,
{
    ring: Arc<RingBuffer<A>>,
    ptr: *mut u8,
    len: usize,
}

impl<A> ViewMut<A>
where
    A: BufferAllocator,
{
    fn new(ring: &Arc<RingBuffer<A>>, offset: usize, len: usize) -> Self {
        Self {
            ring: Arc::clone(ring),
            ptr: (ring.data.as_ptr() as usize + offset) as *mut u8,
            len,
        }
    }
}

impl<A> Deref for ViewMut<A>
where
    A: BufferAllocator,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<A> DerefMut for ViewMut<A>
where
    A: BufferAllocator,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

unsafe impl<A> Send for ViewMut<A> where A: BufferAllocator {}
unsafe impl<A> Sync for ViewMut<A> where A: BufferAllocator {}

/// # Safety
///
/// The underlying buffer of [`View`] must be valid during its lifetime.
pub struct View<A = Global>
where
    A: BufferAllocator,
{
    view: ViewMut<A>,
}

impl<A> View<A>
where
    A: BufferAllocator,
{
    fn new(ring: &Arc<RingBuffer<A>>, offset: usize, len: usize) -> Self {
        let view = ViewMut {
            ring: Arc::clone(ring),
            ptr: (ring.data.as_ptr() as usize + offset) as *mut u8,
            len,
        };
        Self { view }
    }
}

impl<A> Deref for View<A>
where
    A: BufferAllocator,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.view.deref()
    }
}

unsafe impl<A> Send for View<A> where A: BufferAllocator {}
unsafe impl<A> Sync for View<A> where A: BufferAllocator {}
