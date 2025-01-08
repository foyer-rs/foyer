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

use std::{hash::Hash, sync::Arc};

use equivalent::Equivalent;

use crate::{eviction::Eviction, record::Record};

pub trait Indexer: Send + Sync + 'static + Default {
    type Eviction: Eviction;

    fn insert(&mut self, record: Arc<Record<Self::Eviction>>) -> Option<Arc<Record<Self::Eviction>>>;
    fn get<Q>(&self, hash: u64, key: &Q) -> Option<&Arc<Record<Self::Eviction>>>
    where
        Q: Hash + Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized;
    fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<Arc<Record<Self::Eviction>>>
    where
        Q: Hash + Equivalent<<Self::Eviction as Eviction>::Key> + ?Sized;
    fn drain(&mut self) -> impl Iterator<Item = Arc<Record<Self::Eviction>>>;
}

pub mod hash_table;
pub mod sentry;
