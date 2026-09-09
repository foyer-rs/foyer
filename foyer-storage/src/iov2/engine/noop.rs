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

use std::sync::Arc;

use foyer_common::error::Result;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::iov2::engine::{IoEngine, IoEngineBuildContext, IoEngineConfig, IoHandle, IoUnit};
/// Config for a no-operation mock I/O engine.
#[derive(Debug, Default)]
pub struct NoopIoEngineConfig;

impl IoEngineConfig for NoopIoEngineConfig {
    fn build(self: Box<Self>, _: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>> {
        async move { Ok(Arc::new(NoopIoEngine) as Arc<dyn IoEngine>) }.boxed()
    }
}

/// A mock I/O engine that does nothing.
#[derive(Debug)]
pub struct NoopIoEngine;

impl IoEngine for NoopIoEngine {
    fn submit(&self, unit: IoUnit) -> IoHandle {
        async move { (unit.buf, Ok(())) }.boxed().into()
    }
}
