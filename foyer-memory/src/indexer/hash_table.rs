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

use std::{hash::Hash, ptr::NonNull};

use equivalent::Equivalent;
use foyer_common::{code::Key, strict_assert};
use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};

use super::Indexer;
use crate::handle::KeyedHandle;

pub struct HashTableIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = K>,
{
    table: HashTable<NonNull<H>>,
}

unsafe impl<K, H> Send for HashTableIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = K>,
{
}

unsafe impl<K, H> Sync for HashTableIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = K>,
{
}

impl<K, H> Indexer for HashTableIndexer<K, H>
where
    K: Key,
    H: KeyedHandle<Key = K>,
{
    type Key = K;
    type Handle = H;

    fn new() -> Self {
        Self {
            table: HashTable::new(),
        }
    }

    unsafe fn insert(&mut self, mut ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>> {
        let handle = ptr.as_mut();

        strict_assert!(!handle.base().is_in_indexer());
        handle.base_mut().set_in_indexer(true);

        match self.table.entry(
            handle.base().hash(),
            |p| p.as_ref().key() == handle.key(),
            |p| p.as_ref().base().hash(),
        ) {
            HashTableEntry::Occupied(mut o) => {
                std::mem::swap(o.get_mut(), &mut ptr);
                let b = ptr.as_mut().base_mut();
                strict_assert!(b.is_in_indexer());
                b.set_in_indexer(false);
                Some(ptr)
            }
            HashTableEntry::Vacant(v) => {
                v.insert(ptr);
                None
            }
        }
    }

    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Q: Hash + Equivalent<Self::Key> + ?Sized,
    {
        self.table.find(hash, |p| key.equivalent(p.as_ref().key())).copied()
    }

    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Q: Hash + Equivalent<Self::Key> + ?Sized,
    {
        match self
            .table
            .entry(hash, |p| key.equivalent(p.as_ref().key()), |p| p.as_ref().base().hash())
        {
            HashTableEntry::Occupied(o) => {
                let (mut p, _) = o.remove();
                let b = p.as_mut().base_mut();
                strict_assert!(b.is_in_indexer());
                b.set_in_indexer(false);
                Some(p)
            }
            HashTableEntry::Vacant(_) => None,
        }
    }

    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>> {
        self.table.drain().map(|mut ptr| {
            ptr.as_mut().base_mut().set_in_indexer(false);
            ptr
        })
    }
}
