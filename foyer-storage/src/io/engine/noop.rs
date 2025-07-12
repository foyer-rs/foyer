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

use std::sync::Arc;

use futures_util::FutureExt;

use crate::{
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::{noop::NoopDevice, Device, RegionId},
        engine::{IoEngine, IoEngineBuilder, IoHandle},
        error::IoResult,
    },
    Runtime,
};

/// Builder for a no-operation mock I/O engine.
#[derive(Debug, Default)]
pub struct NoopIoEngineBuilder;

impl IoEngineBuilder for NoopIoEngineBuilder {
    fn build(self: Box<Self>, device: Arc<dyn Device>, _: Runtime) -> IoResult<Arc<dyn IoEngine>> {
        Ok(Arc::new(NoopIoEngine(device)))
    }
}

#[derive(Debug)]
pub struct NoopIoEngine(Arc<dyn Device>);

impl Default for NoopIoEngine {
    fn default() -> Self {
        Self(Arc::new(NoopDevice::default()))
    }
}

impl IoEngine for NoopIoEngine {
    fn device(&self) -> &Arc<dyn Device> {
        &self.0
    }

    fn read(&self, buf: Box<dyn IoBufMut>, _: RegionId, _: u64) -> IoHandle {
        async move {
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, Ok(()))
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, _: RegionId, _: u64) -> super::IoHandle {
        async move {
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, Ok(()))
        }
        .boxed()
        .into()
    }
}
