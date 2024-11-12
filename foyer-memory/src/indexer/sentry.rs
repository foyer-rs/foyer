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

use std::{hash::Hash, sync::Arc};

use equivalent::Equivalent;
use foyer_common::strict_assert;

use super::Indexer;
use crate::{eviction::Eviction, record::Record};

/// [`Sentry`] is a guard for all [`Indexer`] implementations to set `IN_INDEXER` flag properly.
pub struct Sentry<I>
where
    I: Indexer,
{
    indexer: I,
}

impl<I> Default for Sentry<I>
where
    I: Indexer,
{
    fn default() -> Self {
        Self { indexer: I::default() }
    }
}

impl<I> Indexer for Sentry<I>
where
    I: Indexer,
{
    type Eviction = I::Eviction;

    fn insert(&mut self, record: Arc<Record<Self::Eviction>>) -> Option<Arc<Record<Self::Eviction>>> {
        strict_assert!(!record.is_in_indexer());
        record.set_in_indexer(true);
        self.indexer.insert(record).inspect(|old| {
            strict_assert!(old.is_in_indexer());
            old.set_in_indexer(false);
        })
    }

    fn get<Q>(&self, hash: u64, key: &Q) -> Option<&Arc<Record<Self::Eviction>>>
    where
        Q: Hash + Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        self.indexer.get(hash, key).inspect(|r| {
            strict_assert!(r.is_in_indexer());
        })
    }

    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<Self::Eviction>>>
    where
        Q: Hash + Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized,
    {
        self.indexer.remove(hash, key).inspect(|r| {
            strict_assert!(r.is_in_indexer());
            r.set_in_indexer(false)
        })
    }

    fn drain(&mut self) -> impl Iterator<Item = Arc<Record<Self::Eviction>>> {
        self.indexer.drain().inspect(|r| {
            strict_assert!(r.is_in_indexer());
            r.set_in_indexer(false)
        })
    }
}
