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

use futures_core::future::BoxFuture;

use crate::{
    Statistics,
    io::{
        bytes::{IoBuf, IoBufMut},
        engine_v2::IoHandle,
    },
};
use foyer_common::{error::Result, metrics::Metrics};

pub struct BlockEngineStorageBuildCtx {
    pub metrics: Arc<Metrics>,
}

pub trait BlockEngineStorageConfig: Debug + Send + Sync + 'static {
    /// Build the storage accessor for block engine from the given configuration.
    fn build(
        self: Box<Self>,
        ctx: BlockEngineStorageBuildCtx,
    ) -> BoxFuture<'static, Result<Arc<dyn BlockEngineStorage>>>;

    /// Box the config.
    fn boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

pub trait BlockEngineStorage: Debug + Send + Sync + 'static {
    fn create_block(&self, size: usize) -> Result<Arc<dyn BlockStorage>>;

    fn statistics(&self) -> &Arc<Statistics>;
}

pub trait BlockStorage: Debug + Send + Sync + 'static {
    fn size(&self) -> usize;

    fn write_at(&self, data: Box<dyn IoBuf>, offset: u64) -> IoHandle<Box<dyn IoBuf>>;

    fn read_at(&self, data: Box<dyn IoBufMut>, offset: u64) -> IoHandle<Box<dyn IoBufMut>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_object_safe(_: &dyn BlockEngineStorage) {}
}
