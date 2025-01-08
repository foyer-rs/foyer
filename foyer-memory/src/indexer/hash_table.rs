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

use std::sync::Arc;

use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};

use super::Indexer;
use crate::{eviction::Eviction, record::Record};

pub struct HashTableIndexer<E>
where
    E: Eviction,
{
    table: HashTable<Arc<Record<E>>>,
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

    fn insert(&mut self, mut record: Arc<Record<Self::Eviction>>) -> Option<Arc<Record<Self::Eviction>>> {
        match self
            .table
            .entry(record.hash(), |r| r.key() == record.key(), |r| r.hash())
        {
            HashTableEntry::Occupied(mut o) => {
                std::mem::swap(o.get_mut(), &mut record);
                Some(record)
            }
            HashTableEntry::Vacant(v) => {
                v.insert(record);
                None
            }
        }
    }

    fn get<Q>(&self, hash: u64, key: &Q) -> Option<&Arc<Record<Self::Eviction>>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        self.table.find(hash, |r| key.equivalent(r.key()))
    }

    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<Self::Eviction>>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        match self.table.entry(hash, |r| key.equivalent(r.key()), |r| r.hash()) {
            HashTableEntry::Occupied(o) => {
                let (r, _) = o.remove();
                Some(r)
            }
            HashTableEntry::Vacant(_) => None,
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = Arc<Record<Self::Eviction>>> {
        self.table.drain()
    }
}
