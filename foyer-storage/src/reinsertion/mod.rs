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

use std::{fmt::Debug, sync::Arc};

use foyer_common::code::{StorageKey, StorageValue};

use crate::{catalog::Catalog, metrics::Metrics};

pub struct ReinsertionContext<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    pub catalog: Arc<Catalog<K, V>>,
    pub metrics: Arc<Metrics>,
}

impl<K, V> Debug for ReinsertionContext<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReinsertionContext").finish()
    }
}

impl<K, V> Clone for ReinsertionContext<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

pub trait ReinsertionPolicy: Send + Sync + 'static {
    type Key: StorageKey;
    type Value: StorageValue;

    fn init(&self, context: ReinsertionContext<Self::Key, Self::Value>);

    fn judge(&self, key: &Self::Key) -> bool;
}

pub mod exist;
pub mod rated_ticket;
