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

use std::{borrow::Borrow, hash::Hash, ptr::NonNull, sync::Arc};

use foyer_common::{arc_key_hash_map::RawArcKeyHashMap, code::Key};

use crate::handle::KeyedHandle;

pub trait Indexer: Send + Sync + 'static {
    type Key: Key;
    type Handle: KeyedHandle<Key = Arc<Self::Key>>;

    fn new() -> Self;
    unsafe fn insert<AK>(&mut self, key: AK, handle: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>>
    where
        AK: Into<Arc<Self::Key>>;
    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>>;
}

pub struct ArcKeyHashMapIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = Arc<K>>,
{
    inner: RawArcKeyHashMap<K, NonNull<H>>,
}

unsafe impl<K, H> Send for ArcKeyHashMapIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = Arc<K>>,
{
}

unsafe impl<K, H> Sync for ArcKeyHashMapIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = Arc<K>>,
{
}

impl<K, H> Indexer for ArcKeyHashMapIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = Arc<K>>,
{
    type Key = K;
    type Handle = H;

    fn new() -> Self {
        Self {
            inner: RawArcKeyHashMap::new(),
        }
    }

    unsafe fn insert<AK>(&mut self, key: AK, mut ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>>
    where
        AK: Into<Arc<Self::Key>>,
    {
        let handle = ptr.as_mut();

        debug_assert!(!handle.base().is_in_indexer());
        handle.base_mut().set_in_indexer(true);

        match self.inner.insert_with_hash(handle.base().hash(), key, ptr) {
            Some(mut old) => {
                let b = old.as_mut().base_mut();
                debug_assert!(b.is_in_indexer());
                b.set_in_indexer(false);
                Some(old)
            }
            None => None,
        }
    }

    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_with_hash(hash, key).copied()
    }

    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove_with_hash(hash, key).map(|mut ptr| {
            ptr.as_mut().base_mut().set_in_indexer(false);
            ptr
        })
    }

    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>> {
        self.inner.drain().map(|(_, mut ptr)| {
            ptr.as_mut().base_mut().set_in_indexer(false);
            ptr
        })
    }
}
