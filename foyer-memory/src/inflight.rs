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

use std::{any::Any, fmt::Debug, hash::Hash};

use equivalent::Equivalent;
use foyer_common::code::{HashBuilder, Key};
use futures_util::future::BoxFuture;
use hashbrown::hash_table::{Entry, HashTable};
use tokio::sync::oneshot;

use crate::{indexer::Indexer, raw::RawCacheEntry, Eviction, Piece};

/// Error type for fetch operations.
pub type FetchError = Box<dyn std::error::Error + Send + Sync + 'static>;
/// Result type for fetch operations.
pub type FetchResult<T> = std::result::Result<T, FetchError>;

/// An optional fetch operation that may return `None` if the entry is not found.
pub type OptionalFetch<T> = BoxFuture<'static, FetchResult<Option<T>>>;
/// A required fetch operation that must return a value.
pub type RequiredFetch<T> = BoxFuture<'static, FetchResult<T>>;

/// A builder for an optional fetch operation.
pub type OptionalFetchBuilder<K, V, P, C> =
    Box<dyn FnOnce(&mut C, &K) -> OptionalFetch<FetchTarget<K, V, P>> + Send + 'static>;
/// A builder for a required fetch operation.
pub type RequiredFetchBuilder<K, V, P, C> =
    Box<dyn FnOnce(&mut C, &K) -> RequiredFetch<FetchTarget<K, V, P>> + Send + 'static>;
/// A type-erased builder for a required fetch operation.
pub type RequiredFetchBuilderErased<K, V, P> =
    Box<dyn FnOnce(&mut dyn Any, &K) -> RequiredFetch<FetchTarget<K, V, P>> + Send + 'static>;

/// A waiter for a fetch operation.
pub type Waiter<T> = oneshot::Receiver<FetchResult<T>>;
/// A notifier for a fetch operation.
pub type Notifier<T> = oneshot::Sender<FetchResult<T>>;

fn erase_required_fetch_builder<K, V, P, C, F>(f: F) -> RequiredFetchBuilderErased<K, V, P>
where
    C: Any + Send + 'static,
    F: FnOnce(&mut C, &K) -> RequiredFetch<FetchTarget<K, V, P>> + Send + 'static,
{
    Box::new(move |ctx, key| {
        let ctx = ctx.downcast_mut::<C>().expect("fetch context type mismatch");
        f(ctx, key)
    })
}

pub fn unerase_required_fetch_builder<K, V, P, C>(
    f: RequiredFetchBuilderErased<K, V, P>,
) -> RequiredFetchBuilder<K, V, P, C>
where
    K: 'static,
    V: 'static,
    P: 'static,
    C: Any + Send + 'static,
{
    Box::new(move |ctx, key| f(ctx as &mut dyn Any, key))
}

/// The target of a fetch operation.
pub enum FetchTarget<K, V, P> {
    /// Fetched entry.
    Entry {
        /// Entry value.
        value: V,
        /// Entry properties.
        properties: P,
    },
    /// Fetched piece from disk cache write queue.
    Piece(Piece<K, V, P>),
}

impl<K, V, P> Debug for FetchTarget<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchTarget").finish()
    }
}

impl<K, V, P> From<Piece<K, V, P>> for FetchTarget<K, V, P> {
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
    // If a required fetch request comes in while there is already an inflight,
    // we store the fetch builder here to let the leader perform the fetch later.
    f: Option<RequiredFetchBuilderErased<E::Key, E::Value, E::Properties>>,
}

struct InflightEntry<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    hash: u64,
    key: E::Key,
    inflight: Inflight<E, S, I>,
}

pub struct InflightManager<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inflights: HashTable<InflightEntry<E, S, I>>,
    next_id: usize,
}

impl<E, S, I> Default for InflightManager<E, S, I>
where
    E: Eviction,
    E::Key: Key,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn default() -> Self {
        Self::new()
    }
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
            inflights: HashTable::new(),
            next_id: 0,
        }
    }

    #[expect(clippy::type_complexity)]
    pub fn enqueue<Q, C>(
        &mut self,
        hash: u64,
        key: &Q,
        f: Option<RequiredFetchBuilder<E::Key, E::Value, E::Properties, C>>,
    ) -> Enqueue<E, S, I, C>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
        C: Any + Send + 'static,
    {
        match self.inflights.entry(hash, |e| key.equivalent(&e.key), |e| e.hash) {
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                if entry.inflight.f.is_none() && f.is_some() {
                    entry.inflight.f = f.map(erase_required_fetch_builder);
                }
                let (tx, rx) = oneshot::channel();
                entry.inflight.notifiers.push(tx);
                Enqueue::Wait(rx)
            }
            Entry::Vacant(v) => {
                let (tx, rx) = oneshot::channel();
                let id = self.next_id;
                self.next_id += 1;
                let entry = InflightEntry {
                    hash,
                    key: key.to_owned(),
                    inflight: Inflight {
                        id,
                        notifiers: vec![tx],
                        f: None,
                    },
                };
                v.insert(entry);
                Enqueue::Lead {
                    id,
                    waiter: rx,
                    required_fetch_builder: f,
                }
            }
        }
    }

    #[expect(clippy::type_complexity)]
    pub fn take<Q>(
        &mut self,
        hash: u64,
        key: &Q,
        id: Option<usize>,
    ) -> Option<Vec<Notifier<Option<RawCacheEntry<E, S, I>>>>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        match self.inflights.entry(hash, |e| key.equivalent(&e.key), |e| e.hash) {
            Entry::Occupied(o) => match id {
                Some(id) if id == o.get().inflight.id => Some(o.remove().0.inflight.notifiers),
                Some(_) => None,
                None => Some(o.remove().0.inflight.notifiers),
            },
            Entry::Vacant(..) => None,
        }
    }

    pub fn fetch_or_take<Q, C>(&mut self, hash: u64, key: &Q, id: usize) -> Option<FetchOrTake<E, S, I, C>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
        C: Any + Send + 'static,
    {
        match self.inflights.entry(hash, |e| key.equivalent(&e.key), |e| e.hash) {
            Entry::Vacant(..) => None,
            Entry::Occupied(mut o) => {
                if o.get().inflight.id != id {
                    return None;
                }
                let f = o.get_mut().inflight.f.take();
                match f.map(unerase_required_fetch_builder) {
                    Some(f) => Some(FetchOrTake::Fetch(f)),
                    None => {
                        let notifiers = o.remove().0.inflight.notifiers;
                        Some(FetchOrTake::Notifiers(notifiers))
                    }
                }
            }
        }
    }
}

#[expect(clippy::type_complexity)]
pub enum Enqueue<E, S, I, C>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
    C: Any + Send + 'static,
{
    Lead {
        id: usize,
        waiter: Waiter<Option<RawCacheEntry<E, S, I>>>,
        required_fetch_builder: Option<RequiredFetchBuilder<E::Key, E::Value, E::Properties, C>>,
    },
    Wait(Waiter<Option<RawCacheEntry<E, S, I>>>),
}

pub enum FetchOrTake<E, S, I, C>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Fetch(RequiredFetchBuilder<E::Key, E::Value, E::Properties, C>),
    Notifiers(Vec<Notifier<Option<RawCacheEntry<E, S, I>>>>),
}
