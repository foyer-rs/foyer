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

use std::ptr::NonNull;

use super::Indexer;
#[cfg(feature = "sanity")]
use crate::handle::Handle;

pub struct SanityIndexer<I>
where
    I: Indexer,
{
    indexer: I,
}

#[cfg(feature = "sanity")]
impl<I> Indexer for SanityIndexer<I>
where
    I: Indexer,
{
    type Key = I::Key;
    type Handle = I::Handle;

    fn new() -> Self {
        Self { indexer: I::new() }
    }

    unsafe fn insert(&mut self, ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>> {
        assert!(!ptr.as_ref().base().is_in_indexer());
        let res = self
            .indexer
            .insert(ptr)
            .inspect(|old| assert!(!old.as_ref().base().is_in_indexer()));
        assert!(ptr.as_ref().base().is_in_indexer());
        res
    }

    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.indexer
            .get(hash, key)
            .inspect(|ptr| assert!(ptr.as_ref().base().is_in_indexer()))
    }

    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.indexer
            .remove(hash, key)
            .inspect(|ptr| assert!(!ptr.as_ref().base().is_in_indexer()))
    }

    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>> {
        self.indexer.drain()
    }
}

#[cfg(not(feature = "sanity"))]
impl<I> Indexer for SanityIndexer<I>
where
    I: Indexer,
{
    type Key = I::Key;
    type Handle = I::Handle;

    fn new() -> Self {
        Self { indexer: I::new() }
    }

    unsafe fn insert(&mut self, handle: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>> {
        self.indexer.insert(handle)
    }

    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.indexer.get(hash, key)
    }

    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.indexer.remove(hash, key)
    }

    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>> {
        self.indexer.drain()
    }
}
