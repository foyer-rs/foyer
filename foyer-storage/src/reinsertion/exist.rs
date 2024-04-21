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

use std::sync::{Arc, OnceLock};

use foyer_common::code::{StorageKey, StorageValue};

use super::{ReinsertionContext, ReinsertionPolicy};
use crate::catalog::Catalog;

#[derive(Debug)]
pub struct ExistReinsertionPolicy<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    catalog: OnceLock<Arc<Catalog<K, V>>>,
}

impl<K, V> Default for ExistReinsertionPolicy<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn default() -> Self {
        Self {
            catalog: OnceLock::new(),
        }
    }
}

impl<K, V> ReinsertionPolicy for ExistReinsertionPolicy<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    type Key = K;

    type Value = V;

    fn init(&self, context: ReinsertionContext<Self::Key, Self::Value>) {
        self.catalog.get_or_init(|| context.catalog.clone());
    }

    fn judge(&self, key: &Self::Key) -> bool {
        let indices = self.catalog.get().unwrap();
        indices.get(key).is_some()
    }
}
