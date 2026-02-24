// Copyright 2026 foyer Project Authors
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

use std::{fmt::Debug, sync::Arc};

use foyer_common::{
    code::{StorageKey, StorageValue},
    error::Result,
    properties::Properties,
};
use futures_core::future::BoxFuture;

use crate::{
    engine::{Engine, Load},
    filter::StorageFilterResult,
    io::device::Device,
    keeper::PieceRef,
};

pub struct ObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    _marker: std::marker::PhantomData<(K, V, P)>,
    // inner: Arc<BlockEngineInner<K, V, P>>,
}

impl<K, V, P> Debug for ObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectEngine").finish()
    }
}

impl<K, V, P> Engine<K, V, P> for ObjectEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn device(&self) -> &Arc<dyn Device> {
        todo!()
    }

    fn filter(&self, hash: u64, estimated_size: usize) -> StorageFilterResult {
        todo!()
    }

    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
        todo!()
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V, P>>> {
        todo!()
    }

    fn delete(&self, hash: u64) {
        todo!()
    }

    fn may_contains(&self, hash: u64) -> bool {
        todo!()
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        todo!()
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        todo!()
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        todo!()
    }
}
