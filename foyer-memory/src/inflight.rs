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

use crate::{error::Result, eviction::Eviction, indexer::Indexer, pipe::Piece, raw::RawCacheEntry};
use equivalent::Equivalent;
use foyer_common::code::HashBuilder;
use futures_util::{future::BoxFuture, FutureExt};
use hashbrown::hash_map::{Entry as HashMapEntry, HashMap};
use std::hash::Hash;
use tokio::sync::oneshot;

pub enum FetchTarget<K, V, P> {
    Entry { key: K, value: V, properties: P },
    Piece(Piece<K, V, P>),
}

pub type BoxOptionalFetchFuture<K, V, P> = BoxFuture<'static, Result<Option<FetchTarget<K, V, P>>>>;
pub type BoxRequiredFetchFuture<K, V, P> = BoxFuture<'static, Result<FetchTarget<K, V, P>>>;

pub enum BoxFetchFuture<K, V, P> {
    Optional(BoxOptionalFetchFuture<K, V, P>),
    Required(BoxRequiredFetchFuture<K, V, P>),
}

pub type Waiter<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
= oneshot::Receiver<Option<RawCacheEntry<E, S, I>>>;
pub type Notifier<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
= oneshot::Sender<Option<RawCacheEntry<E, S, I>>>;

pub enum Wait<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Waiter(Waiter<E, S, I>),
    Optional(BoxOptionalFetchFuture<E::Key, E::Value, E::Properties>),
    Required(BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>),
}

pub enum TakeOrUpgrade<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Notifiers(Vec<Notifier<E, S, I>>),
    Required(BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>),
}

pub enum Inflight<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    Optional {
        notifiers: Vec<Notifier<E, S, I>>,
        next: Option<BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>>,
    },
    Required {
        notifiers: Vec<Notifier<E, S, I>>,
    },
}

pub struct InflightMap<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    inflights: HashMap<E::Key, Inflight<E, S, I>>,
}

impl<E, S, I> Default for InflightMap<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    fn default() -> Self {
        Self {
            inflights: HashMap::new(),
        }
    }
}

impl<E, S, I> InflightMap<E, S, I>
where
    E: Eviction,
    S: HashBuilder,
    I: Indexer<Eviction = E>,
{
    pub fn take<Q>(&mut self, key: &Q) -> Vec<Notifier<E, S, I>>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized,
    {
        match self.inflights.remove(key) {
            Some(Inflight::Optional { notifiers: waiters, .. }) | Some(Inflight::Required { notifiers: waiters }) => {
                waiters
            }
            None => vec![],
        }
    }

    pub fn take_or_upgrade<Q>(&mut self, key: &Q) -> TakeOrUpgrade<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
    {
        match self.inflights.remove(key) {
            // Unreachable because only one inflight request can be issued.
            // If this function is called, it must be an optional fetch being upgraded to required.
            Some(Inflight::Required { .. }) => unreachable!(),
            // Handled by other concurrent insertions.
            None => TakeOrUpgrade::Notifiers(vec![]),
            Some(Inflight::Optional { notifiers, next: None }) => TakeOrUpgrade::Notifiers(notifiers),
            Some(Inflight::Optional {
                notifiers,
                next: Some(fr),
            }) => {
                self.inflights.insert(key.to_owned(), Inflight::Required { notifiers });
                TakeOrUpgrade::Required(fr)
            }
        }
    }

    pub fn wait<Q>(
        &mut self,
        key: &Q,
        fo: Option<Box<dyn FnOnce(&Q) -> BoxOptionalFetchFuture<E::Key, E::Value, E::Properties>>>,
        fr: Option<Box<dyn FnOnce(&Q) -> BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>>>,
    ) -> Wait<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
    {
        match (fo, fr) {
            (None, None) => panic!("Either optional or required fetch function must be provided"),
            (Some(fo), None) => self.wait_optional(key, fo),
            (None, Some(fr)) => self.wait_required(key, fr),
            (Some(fo), Some(fr)) => self.wait_both(key, fo, fr),
        }
    }

    fn wait_optional<Q>(
        &mut self,
        key: &Q,
        f: Box<dyn FnOnce(&Q) -> BoxOptionalFetchFuture<E::Key, E::Value, E::Properties>>,
    ) -> Wait<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
    {
        match self.inflights.entry(key.to_owned()) {
            HashMapEntry::Vacant(v) => {
                let future = f(key);
                v.insert(Inflight::Optional {
                    notifiers: vec![],
                    next: None,
                });
                Wait::Optional(future)
            }
            HashMapEntry::Occupied(mut o) => {
                let (tx, rx) = oneshot::channel();
                match o.get_mut() {
                    Inflight::Optional { notifiers: waiters, .. } | Inflight::Required { notifiers: waiters } => {
                        waiters.push(tx);
                        Wait::Waiter(rx)
                    }
                }
            }
        }
    }

    fn wait_required<Q>(
        &mut self,
        key: &Q,
        f: Box<dyn FnOnce(&Q) -> BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>>,
    ) -> Wait<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
    {
        match self.inflights.entry(key.to_owned()) {
            HashMapEntry::Vacant(v) => {
                let future = f(key);
                v.insert(Inflight::Required { notifiers: vec![] });
                Wait::Required(future)
            }
            HashMapEntry::Occupied(mut o) => {
                let (tx, rx) = oneshot::channel();
                if let Inflight::Optional { next, .. } = o.get_mut() {
                    if next.is_none() {
                        let future = f(key);
                        *next = Some(future);
                    }
                }
                match o.get_mut() {
                    Inflight::Optional { notifiers: waiters, .. } | Inflight::Required { notifiers: waiters } => {
                        waiters.push(tx);
                        Wait::Waiter(rx)
                    }
                }
            }
        }
    }

    fn wait_both<Q>(
        &mut self,
        key: &Q,
        fo: Box<dyn FnOnce(&Q) -> BoxOptionalFetchFuture<E::Key, E::Value, E::Properties>>,
        fr: Box<dyn FnOnce(&Q) -> BoxRequiredFetchFuture<E::Key, E::Value, E::Properties>>,
    ) -> Wait<E, S, I>
    where
        Q: Hash + Equivalent<E::Key> + ?Sized + ToOwned<Owned = E::Key>,
    {
        match self.inflights.entry(key.to_owned()) {
            HashMapEntry::Vacant(v) => {
                let fo = fo(key);
                let fr = fr(key);
                let f = async {
                    match fo.await {
                        Ok(Some(t)) => Ok(t),
                        Ok(None) => fr.await,
                        Err(e) => Err(e),
                    }
                }
                .boxed();
                v.insert(Inflight::Required { notifiers: vec![] });
                Wait::Required(f)
            }
            HashMapEntry::Occupied(mut o) => {
                let (tx, rx) = oneshot::channel();
                if let Inflight::Optional { next, .. } = o.get_mut() {
                    if next.is_none() {
                        let future = fr(key);
                        *next = Some(future);
                    }
                }
                match o.get_mut() {
                    Inflight::Optional { notifiers: waiters, .. } | Inflight::Required { notifiers: waiters } => {
                        waiters.push(tx);
                        Wait::Waiter(rx)
                    }
                }
            }
        }
    }
}
