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

use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use equivalent::Equivalent;
use foyer_common::code::{HashBuilder, Key};
use futures_util::future::BoxFuture;
use hashbrown::hash_map::{EntryRef as HashMapEntryRef, HashMap};
use tokio::sync::oneshot;

use crate::{indexer::Indexer, raw::RawCacheEntry, Eviction, Piece};

pub type FetchError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type FetchResult<T> = std::result::Result<T, FetchError>;

pub type OptionalFetch<T> = BoxFuture<'static, FetchResult<Option<T>>>;
pub type RequiredFetch<T> = BoxFuture<'static, FetchResult<T>>;

pub type OptionalFetchBuilder<K, V, P> =
    Box<dyn FnOnce(&Arc<dyn Any + Send + Sync + 'static>, K) -> OptionalFetch<FetchTargetV2<K, V, P>> + Send + 'static>;
pub type RequiredFetchBuilder<K, V, P> =
    Box<dyn FnOnce(&Arc<dyn Any + Send + Sync + 'static>, K) -> RequiredFetch<FetchTargetV2<K, V, P>> + Send + 'static>;

pub type Waiter<T> = oneshot::Receiver<FetchResult<T>>;
pub type Notifier<T> = oneshot::Sender<FetchResult<T>>;

/// The target of a fetch operation.
pub enum FetchTargetV2<K, V, P> {
    /// Fetched entry.
    Entry {
        /// Entry key.
        key: K,
        /// Entry value.
        value: V,
        /// Entry properties.
        properties: P,
    },
    /// Fetched piece from disk cache write queue.
    Piece(Piece<K, V, P>),
}

impl<K, V, P> Debug for FetchTargetV2<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchTarget").finish()
    }
}

impl<K, V, P> From<Piece<K, V, P>> for FetchTargetV2<K, V, P> {
    fn from(piece: Piece<K, V, P>) -> Self {
        Self::Piece(piece)
    }
}

struct Inflight<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    id: usize,
    notifiers: Vec<Notifier<Option<RawCacheEntry<E, S, I>>>>,
    f: Option<RequiredFetchBuilder<E::Key, E::Value, E::Properties>>,
}

pub struct InflightManager<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inflights: HashMap<E::Key, Inflight<E, S, I>>,
    next_id: usize,
}

impl<E, S, I> InflightManager<E, S, I>
where
    E: Eviction,
    E::Key: Key,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn new() -> Self {
        Self {
            inflights: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn enqueue<'a, 'b, Q>(
        &'a mut self,
        key: &'b Q,
        f: Option<RequiredFetchBuilder<E::Key, E::Value, E::Properties>>,
    ) -> Enqueue<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
        &'b Q: Into<E::Key>,
    {
        match self.inflights.entry_ref(key) {
            HashMapEntryRef::Occupied(mut o) => {
                let inflight = o.get_mut();
                if inflight.f.is_none() && f.is_some() {
                    inflight.f = f;
                }
                let (tx, rx) = oneshot::channel();
                inflight.notifiers.push(tx);
                Enqueue::Wait(rx)
            }
            HashMapEntryRef::Vacant(v) => {
                let (tx, rx) = oneshot::channel();
                let id = self.next_id;
                self.next_id += 1;
                let inflight = Inflight {
                    id,
                    notifiers: vec![tx],
                    f,
                };
                v.insert(inflight);
                Enqueue::Lead { id, waiter: rx }
            }
        }
    }

    pub fn take<Q>(&mut self, key: &Q, id: Option<usize>) -> Option<Vec<Notifier<Option<RawCacheEntry<E, S, I>>>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        match id {
            Some(id) => {
                let inflight = self.inflights.get(key)?;
                if inflight.id != id {
                    return None;
                }
                self.inflights.remove(key).map(|inflight| inflight.notifiers)
            }
            None => self.inflights.remove(key).map(|inflight| inflight.notifiers),
        }
    }

    pub fn fetch_or_take<Q>(&mut self, key: &Q, id: usize) -> Option<FetchOrTake<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        let inflight = self.inflights.get_mut(key)?;
        if inflight.id != id {
            return None;
        }
        let fetch = inflight.f.take();
        if let Some(fetch) = fetch {
            return Some(FetchOrTake::Fetch(fetch));
        }
        let notifiers = self.inflights.remove(key).map(|inflight| inflight.notifiers)?;
        Some(FetchOrTake::Notifiers(notifiers))
    }
}

pub enum Enqueue<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Lead {
        id: usize,
        waiter: Waiter<Option<RawCacheEntry<E, S, I>>>,
    },
    Wait(Waiter<Option<RawCacheEntry<E, S, I>>>),
}

pub enum FetchOrTake<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Fetch(RequiredFetchBuilder<E::Key, E::Value, E::Properties>),
    Notifiers(Vec<Notifier<Option<RawCacheEntry<E, S, I>>>>),
}
