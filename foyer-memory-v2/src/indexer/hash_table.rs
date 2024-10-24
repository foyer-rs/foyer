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

use std::ptr::NonNull;

use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};

use crate::{Eviction, Record};

use super::Indexer;

pub struct HashTableIndexer<E>
where
    E: Eviction,
{
    table: HashTable<NonNull<Record<E>>>,
}

unsafe impl<E> Send for HashTableIndexer<E> where E: Eviction {}
unsafe impl<E> Sync for HashTableIndexer<E> where E: Eviction {}

impl<E> Default for HashTableIndexer<E>
where
    E: Eviction,
{
    fn default() -> Self {
        Self {
            table: Default::default(),
        }
    }
}

impl<E> Indexer for HashTableIndexer<E>
where
    E: Eviction,
{
    type Eviction = E;

    fn insert(&mut self, mut ptr: NonNull<Record<Self::Eviction>>) -> Option<NonNull<Record<Self::Eviction>>> {
        let record = unsafe { ptr.as_ref() };

        match self.table.entry(
            record.hash(),
            |p| unsafe { p.as_ref() }.key() == record.key(),
            |p| unsafe { p.as_ref() }.hash(),
        ) {
            HashTableEntry::Occupied(mut o) => {
                std::mem::swap(o.get_mut(), &mut ptr);
                Some(ptr)
            }
            HashTableEntry::Vacant(v) => {
                v.insert(ptr);
                None
            }
        }
    }

    fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Record<Self::Eviction>>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        self.table
            .find(hash, |p| key.equivalent(unsafe { p.as_ref() }.key()))
            .copied()
    }

    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Record<Self::Eviction>>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        match self.table.entry(
            hash,
            |p| key.equivalent(unsafe { p.as_ref() }.key()),
            |p| unsafe { p.as_ref() }.hash(),
        ) {
            HashTableEntry::Occupied(o) => {
                let (p, _) = o.remove();
                Some(p)
            }
            HashTableEntry::Vacant(_) => None,
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = NonNull<Record<Self::Eviction>>> {
        self.table.drain()
    }
}
