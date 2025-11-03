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

use std::{fmt::Debug, future::Future, hash::Hash, ops::Not};

use equivalent::Equivalent;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    utils::either::Either,
};
use futures_core::future::BoxFuture;
use hashbrown::hash_map::{Entry as HashMapEntry, EntryRef as HashMapEntryRef, HashMap};
use tokio::sync::oneshot;

use crate::{
    hybrid::error::{Error, Result},
    HybridCache, HybridCacheEntry, HybridCacheProperties,
};

pub type DiskCacheLookup<'a, T> = BoxFuture<'a, Result<Option<T>>>;
pub type RemoteFetch<'a, T> = BoxFuture<'a, Result<T>>;

pub type Notifier<T> = oneshot::Sender<Result<T>>;
pub type Waiter<T> = oneshot::Receiver<Result<T>>;

pub enum Launch<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Wait(Waiter<Option<HybridCacheEntry<K, V, S>>>),
}

pub struct InflightTask<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    id: usize,
    remote: Option<RemoteFetch<'static, (K, V, HybridCacheProperties)>>,
    notifiers: Vec<Notifier<Option<HybridCacheEntry<K, V, S>>>>,
}

pub struct InflightMap<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    tasks: HashMap<K, InflightTask<K, V, S>>,
    id: usize,
}

impl<K, V, S> InflightMap<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    // /// If [`None`] is returned, the caller is responsible for launching the load/fetch.
    // /// If [`Some`] is returned, the caller should wait on the returned waiter
    // pub fn enqueue<'a, 'b, Q>(&'a mut self, key: &'b Q) -> Option<Waiter<Option<HybridCacheEntry<K, V, S>>>>
    // where
    //     Q: Hash + Equivalent<K> + ?Sized,
    //     &'b Q: Into<K>,
    // {
    //     match self.tasks.entry_ref(key) {
    //         HashMapEntryRef::Vacant(v) => {
    //             v.insert(InflightTask {
    //                 remote: None,
    //                 notifiers: vec![],
    //             });
    //             None
    //         }
    //         HashMapEntryRef::Occupied(mut o) => {
    //             let (tx, rx) = oneshot::channel();
    //             o.get_mut().notifiers.push(tx);
    //             Some(rx)
    //         }
    //     }
    // }

    /// If [`Either::Left`] is returned, the caller is responsible for launching the load/fetch.
    /// If [`Either::Right`] is returned, the caller should wait on the returned waiter
    pub fn enqueue<'a, 'b, Q>(
        &'a mut self,
        key: &'b Q,
        remote: Option<RemoteFetch<'static, (K, V, HybridCacheProperties)>>,
    ) -> Either<(usize, Waiter<Option<HybridCacheEntry<K, V, S>>>), Waiter<Option<HybridCacheEntry<K, V, S>>>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
        &'b Q: Into<K>,
    {
        let (tx, rx) = oneshot::channel();
        match self.tasks.entry_ref(key) {
            HashMapEntryRef::Vacant(v) => {
                let id = self.id;
                v.insert(InflightTask {
                    id,
                    remote,
                    notifiers: vec![tx],
                });
                self.id += 1;
                Either::Left((id, rx))
            }
            HashMapEntryRef::Occupied(mut o) => {
                let task = o.get_mut();
                task.notifiers.push(tx);
                if let Some(remote) = remote {
                    task.remote.get_or_insert(remote);
                }
                Either::Right(rx)
            }
        }
    }

    pub fn take<Q>(&mut self, key: &Q, id: Option<usize>) -> Vec<Notifier<Option<HybridCacheEntry<K, V, S>>>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        match id {
            Some(seq) => {
                let task = match self.tasks.get(key) {
                    Some(t) => t,
                    None => return vec![],
                };
                if task.id == seq {
                    self.tasks.remove(key).map(|t| t.notifiers).unwrap_or_default()
                } else {
                    vec![]
                }
            }
            None => self.tasks.remove(key).map(|t| t.notifiers).unwrap_or_default(),
        }
    }

    pub fn remote<Q>(&mut self, key: &Q, id: usize) -> Option<RemoteFetch<'static, (K, V, HybridCacheProperties)>>
    where
        Q: Hash + Equivalent<K> + ?Sized,
    {
        self.tasks
            .get_mut(key)
            .and_then(|t| if t.id == id { t.remote.take() } else { None })
    }

    // pub fn launch_disk<'a, 'b, Q, F, FU>(&'a mut self, key: &'b Q) -> Waiter<Option<HybridCacheEntry<K, V, S>>>
    // where
    //     Q: Hash + Equivalent<K> + ?Sized,
    //     &'b Q: Into<K>,
    // {
    //     let (tx, rx) = oneshot::channel();
    //     match self.tasks.entry_ref(key) {
    //         HashMapEntryRef::Occupied(mut o) => o.get_mut().notifiers.push(tx),
    //         HashMapEntryRef::Vacant(v) => {
    //             v.insert(InflightTask {
    //                 remote: None,
    //                 notifiers: vec![tx],
    //             });
    //             let load = self.cache.storage().load(key);
    //         }
    //     }
    //     rx
    // }

    // pub fn launch_remote<Q, F, FU>(&mut self, key: &Q, fetch: F)
    // where
    //     Q: Hash + Equivalent<K> + ?Sized + ToOwned<Owned = K>,
    //     F: FnOnce() -> FU,
    //     FU: Future<Output = Result<V>> + 'static + Send,
    // {
    // }
}
