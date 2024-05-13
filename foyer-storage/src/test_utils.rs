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

use std::{borrow::Borrow, collections::HashSet, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use foyer_common::code::StorageKey;
use parking_lot::Mutex;

use crate::{
    picker::{AdmissionPicker, ReinsertionPicker},
    statistics::Statistics,
};

pub struct BiasedPicker<K, Q> {
    admits: HashSet<Q>,
    _marker: PhantomData<K>,
}

impl<K, Q> Debug for BiasedPicker<K, Q>
where
    Q: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BiasedPicker").field("admits", &self.admits).finish()
    }
}

impl<K, Q> BiasedPicker<K, Q> {
    pub fn new(admits: impl IntoIterator<Item = Q>) -> Self
    where
        Q: Hash + Eq,
    {
        Self {
            admits: admits.into_iter().collect(),
            _marker: PhantomData,
        }
    }
}

impl<K, Q> AdmissionPicker for BiasedPicker<K, Q>
where
    K: Send + Sync + 'static + Borrow<Q>,
    Q: Hash + Eq + Send + Sync + 'static + Debug,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, key: &Self::Key) -> bool {
        self.admits.contains(key.borrow())
    }
}

impl<K, Q> ReinsertionPicker for BiasedPicker<K, Q>
where
    K: Send + Sync + 'static + Borrow<Q>,
    Q: Hash + Eq + Send + Sync + 'static + Debug,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, key: &Self::Key) -> bool {
        self.admits.contains(key.borrow())
    }
}

#[derive(Debug, Clone)]
pub enum Record<K> {
    Admit(K),
    Evict(K),
}

pub struct JudgeRecorder<K> {
    records: Mutex<Vec<Record<K>>>,
}

impl<K> Debug for JudgeRecorder<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JudgeRecorder").finish()
    }
}

impl<K> JudgeRecorder<K>
where
    K: StorageKey + Clone,
{
    pub fn dump(&self) -> Vec<Record<K>> {
        self.records.lock().clone()
    }

    pub fn remains(&self) -> HashSet<K> {
        let records = self.dump();
        let mut res = HashSet::default();
        for record in records {
            match record {
                Record::Admit(key) => {
                    res.insert(key);
                }
                Record::Evict(key) => {
                    res.remove(&key);
                }
            }
        }
        res
    }
}

impl<K> Default for JudgeRecorder<K>
where
    K: StorageKey,
{
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::default()),
        }
    }
}

impl<K> AdmissionPicker for JudgeRecorder<K>
where
    K: StorageKey + Clone,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, key: &Self::Key) -> bool {
        self.records.lock().push(Record::Admit(key.clone()));
        true
    }
}

impl<K> ReinsertionPicker for JudgeRecorder<K>
where
    K: StorageKey + Clone,
{
    type Key = K;

    fn pick(&self, _: &Arc<Statistics>, key: &Self::Key) -> bool {
        self.records.lock().push(Record::Evict(key.clone()));
        false
    }
}
