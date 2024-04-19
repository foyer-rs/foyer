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
//  limitations under the License.s

use std::{collections::HashSet, marker::PhantomData};

use foyer_common::code::{StorageKey, StorageValue};
use parking_lot::Mutex;

use crate::{
    admission::{AdmissionContext, AdmissionPolicy},
    reinsertion::{ReinsertionContext, ReinsertionPolicy},
};

#[derive(Debug, Clone)]
pub enum Record<K: StorageKey> {
    Admit(K),
    Evict(K),
}

#[derive(Debug)]
pub struct JudgeRecorder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    records: Mutex<Vec<Record<K>>>,
    _marker: PhantomData<V>,
}

impl<K, V> JudgeRecorder<K, V>
where
    K: StorageKey + Clone,
    V: StorageValue + Clone,
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

impl<K, V> Default for JudgeRecorder<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::default()),
            _marker: PhantomData,
        }
    }
}

impl<K, V> AdmissionPolicy for JudgeRecorder<K, V>
where
    K: StorageKey + Clone,
    V: StorageValue + Clone,
{
    type Key = K;

    type Value = V;

    fn init(&self, _: AdmissionContext<Self::Key, Self::Value>) {}

    fn judge(&self, key: &K) -> bool {
        self.records.lock().push(Record::Admit(key.clone()));
        true
    }
}

impl<K, V> ReinsertionPolicy for JudgeRecorder<K, V>
where
    K: StorageKey + Clone,
    V: StorageValue + Clone,
{
    type Key = K;

    type Value = V;

    fn init(&self, _: ReinsertionContext<Self::Key, Self::Value>) {}

    fn judge(&self, key: &K) -> bool {
        self.records.lock().push(Record::Evict(key.clone()));
        false
    }
}
