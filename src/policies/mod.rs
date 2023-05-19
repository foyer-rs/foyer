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

//! A collection of cache eviction policies.
//!
//! Cache eviction policies only cares about the order of cached entries.
//! They don't store the real cache entries or resource usage.

pub mod lru;
pub mod tinylfu;

use std::{hash::Hash, ptr::NonNull};

pub trait Index: Clone + Hash + Send + Sync + 'static {}

pub trait Policy: Send + Sync + 'static {
    type I: Index;

    fn add(&mut self, handle: NonNull<Handle<Self::I>>) -> bool;

    fn remove(&mut self, handle: NonNull<Handle<Self::I>>) -> bool;

    fn record_access(&mut self, handle: NonNull<Handle<Self::I>>, mode: AccessMode) -> bool;

    fn eviction_iter(&mut self) -> EvictionIter<'_, Self::I>;
}

pub enum EvictionIter<'a, I: Index> {
    LruEvictionIter(lru::EvictionIter<'a, I>),
    TinyLfuEvictionIter(tinylfu::EvictionIter<'a, I>),
}

impl<'a, I: Index> Iterator for EvictionIter<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EvictionIter::LruEvictionIter(iter) => iter.next(),
            EvictionIter::TinyLfuEvictionIter(iter) => iter.next(),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum AccessMode {
    Read,
    Write,
}

pub enum Handle<I: Index> {
    LruHandle(lru::Handle<I>),
    TinyLfuHandle(tinylfu::Handle<I>),
}

impl<I: Index> Handle<I> {
    pub fn index(&self) -> &I {
        match self {
            Handle::LruHandle(handle) => handle.index(),
            Handle::TinyLfuHandle(handle) => handle.index(),
        }
    }
}

#[macro_export]
macro_rules! extract_handle {
    ($handle:expr, $type:tt) => {
        paste::paste! {
            unsafe {
                let inner = match $handle.as_mut() {
                    $crate::policies::Handle::[<$type Handle>](handle) => handle,
                    _ => unreachable!(),
                };
                std::ptr::NonNull::new_unchecked(inner as *mut _)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Index for u64 {}
}
