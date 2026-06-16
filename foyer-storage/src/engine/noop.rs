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

use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use equivalent::Equivalent;
use foyer_common::{
    code::{StorageKey, StorageValue},
    error::Result,
    properties::Properties,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::{
    Device, DeviceBuilder, Load, NoopDeviceBuilder, StorageFilterResult,
    engine::{Engine, EngineBuildContext, EngineConfig},
    keeper::PieceRef,
};

/// Config for the noop disk cache engine that does nothing.
pub struct NoopEngineConfig<K, V, P>(PhantomData<(K, V, P)>)
where
    K: StorageKey,
    V: StorageValue,
    P: Properties;

impl<K, V, P> Debug for NoopEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoopEngineConfig").finish()
    }
}

impl<K, V, P> Default for NoopEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K, V, P> NoopEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// Build the noop engine.
    pub fn build(self) -> Arc<NoopEngine<K, V, P>> {
        let device = NoopDeviceBuilder::default().build().unwrap();
        let device: Arc<dyn Device> = device;
        Arc::new(NoopEngine {
            device,
            marker: PhantomData,
        })
    }
}

impl<K, V, P> EngineConfig<K, V, P> for NoopEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    type Engine = NoopEngine<K, V, P>;

    fn build(self: Box<Self>, _: EngineBuildContext) -> BoxFuture<'static, Result<Arc<NoopEngine<K, V, P>>>> {
        async move { Ok((*self).build()) }.boxed()
    }
}

/// Noop disk cache engine that does nothing.
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
    const IS_NOOP: bool = true;

    fn device(&self) -> &Arc<dyn Device> {
        &self.device
    }

    fn filter(&self, _: u64, _: usize) -> StorageFilterResult {
        StorageFilterResult::Reject
    }

    fn enqueue(&self, _: PieceRef<K, V, P>, _: usize) {}

    async fn load<'a, Q>(&'a self, _: u64, _: &'a Q) -> Result<Load<K, V, P>>
    where
        Q: Hash + Equivalent<K> + Sync + ?Sized,
    {
        Ok(Load::Miss)
    }

    fn delete<Q>(&self, _: u64, _: &Q)
    where
        Q: Hash + Equivalent<K> + Sync + ?Sized,
    {
    }

    fn may_contains<Q>(&self, _: u64, _: &Q) -> bool
    where
        Q: Hash + Equivalent<K> + Sync + ?Sized,
    {
        false
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }

    async fn wait(&self) {}

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
