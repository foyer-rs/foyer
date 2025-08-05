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

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use foyer_common::{
    code::{StorageKey, StorageValue},
    properties::Properties,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::{
    engine::{Engine, EngineBuildContext, EngineConfig},
    error::Result,
    keeper::PieceRef,
    Device, DeviceBuilder, FilterResult, Load, NoopDeviceBuilder,
};

pub struct NoopEngineBuilder<K, V, P>(PhantomData<(K, V, P)>)
where
    K: StorageKey,
    V: StorageValue,
    P: Properties;

impl<K, V, P> Debug for NoopEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoopEngineBuilder").finish()
    }
}

impl<K, V, P> Default for NoopEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K, V, P> NoopEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn build(self) -> Arc<NoopEngine<K, V, P>> {
        let device = NoopDeviceBuilder::default().build().unwrap();
        let device: Arc<dyn Device> = device;
        Arc::new(NoopEngine {
            device,
            marker: PhantomData,
        })
    }
}

impl<K, V, P> EngineConfig<K, V, P> for NoopEngineBuilder<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn build(self: Box<Self>, _: EngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>> {
        async move { Ok((*self).build() as Arc<dyn Engine<K, V, P>>) }.boxed()
    }
}

impl<K, V, P> From<NoopEngineBuilder<K, V, P>> for Box<dyn EngineConfig<K, V, P>>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn from(builder: NoopEngineBuilder<K, V, P>) -> Self {
        builder.boxed()
    }
}

pub struct NoopEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    device: Arc<dyn Device>,
    marker: PhantomData<(K, V, P)>,
}

impl<K, V, P> Debug for NoopEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoopEngine").finish()
    }
}

impl<K, V, P> Engine<K, V, P> for NoopEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn device(&self) -> &Arc<dyn Device> {
        &self.device
    }

    fn filter(&self, _: u64, _: usize) -> FilterResult {
        FilterResult::Reject
    }

    fn enqueue(&self, _: PieceRef<K, V, P>, _: usize) {}

    fn load(&self, _: u64) -> BoxFuture<'static, Result<Load<K, V, P>>> {
        async move { Ok(Load::Miss) }.boxed()
    }

    fn delete(&self, _: u64) {}

    fn may_contains(&self, _: u64) -> bool {
        false
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        async move { Ok(()) }.boxed()
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        async move {}.boxed()
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        async move { Ok(()) }.boxed()
    }
}
