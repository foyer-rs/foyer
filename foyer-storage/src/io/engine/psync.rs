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

use std::{
    fmt::Debug,
    fs::File,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[cfg(feature = "tracing")]
use fastrace::prelude::*;
use foyer_common::{
    error::{Error, Result},
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::{
    io::{
        bytes::{IoB, IoBuf, IoBufMut, Raw},
        device::Partition,
        engine::{IoEngine, IoEngineBuildContext, IoEngineConfig, IoHandle},
    },
    RawFile,
};

#[derive(Debug)]
struct FileHandle(ManuallyDrop<File>);

#[cfg(target_family = "windows")]
impl From<RawFile> for FileHandle {
    fn from(raw: RawFile) -> Self {
        use std::os::windows::io::FromRawHandle;
        let file = unsafe { File::from_raw_handle(raw.0) };
        let file = ManuallyDrop::new(file);
        Self(file)
    }
}

#[cfg(target_family = "unix")]
impl From<RawFile> for FileHandle {
    fn from(raw: RawFile) -> Self {
        use std::os::unix::io::FromRawFd;
        let file = unsafe { File::from_raw_fd(raw.0) };
        let file = ManuallyDrop::new(file);
        Self(file)
    }
}

impl Deref for FileHandle {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FileHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Config for synchronous I/O engine with pread(2)/pwrite(2).
#[derive(Debug)]
pub struct PsyncIoEngineConfig {
    #[cfg(any(test, feature = "test_utils"))]
    write_io_latency: Option<std::ops::Range<std::time::Duration>>,

    #[cfg(any(test, feature = "test_utils"))]
    read_io_latency: Option<std::ops::Range<std::time::Duration>>,
}

impl Default for PsyncIoEngineConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl From<PsyncIoEngineConfig> for Box<dyn IoEngineConfig> {
    fn from(builder: PsyncIoEngineConfig) -> Self {
        builder.boxed()
    }
}

impl PsyncIoEngineConfig {
    /// Create a new synchronous I/O engine config with default configurations.
    pub fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test_utils"))]
            write_io_latency: None,
            #[cfg(any(test, feature = "test_utils"))]
            read_io_latency: None,
        }
    }

    /// Set the simulated additional write I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_write_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.write_io_latency = Some(latency);
        self
    }

    /// Set the simulated additional read I/O latency for testing purposes.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_read_io_latency(mut self, latency: std::ops::Range<std::time::Duration>) -> Self {
        self.read_io_latency = Some(latency);
        self
    }
}

impl IoEngineConfig for PsyncIoEngineConfig {
    fn build(self: Box<Self>, ctx: IoEngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn IoEngine>>> {
        async move {
            let engine = PsyncIoEngine {
                spawner: ctx.spawner,
                #[cfg(any(test, feature = "test_utils"))]
                write_io_latency: None,
                #[cfg(any(test, feature = "test_utils"))]
                read_io_latency: None,
            };
            let engine: Arc<dyn IoEngine> = Arc::new(engine);
            Ok(engine)
        }
        .boxed()
    }
}

/// The synchronous I/O engine that uses pread(2)/pwrite(2) and tokio thread pool for reading and writing.
pub struct PsyncIoEngine {
    spawner: Spawner,

    #[cfg(any(test, feature = "test_utils"))]
    write_io_latency: Option<std::ops::Range<std::time::Duration>>,
    #[cfg(any(test, feature = "test_utils"))]
    read_io_latency: Option<std::ops::Range<std::time::Duration>>,
}

impl Debug for PsyncIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PsyncIoEngine").finish()
    }
}

impl IoEngine for PsyncIoEngine {
    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::psync::read")
    )]
    fn read(&self, buf: Box<dyn IoBufMut>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (raw, offset) = partition.translate(offset);
        let file = FileHandle::from(raw);
        let runtime = self.spawner.clone();

        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::psync::read::io");

        #[cfg(any(test, feature = "test_utils"))]
        let read_io_latency = self.read_io_latency.clone();
        async move {
            let (buf, res) = match runtime
                .spawn_blocking(move || {
                    let (ptr, len) = buf.as_raw_parts();
                    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                    let res = {
                        #[cfg(target_family = "windows")]
                        {
                            use std::os::windows::fs::FileExt;
                            file.seek_read(slice, offset).map(|_| ()).map_err(Error::io_error)
                        }
                        #[cfg(target_family = "unix")]
                        {
                            use std::os::unix::fs::FileExt;
                            file.read_exact_at(slice, offset).map_err(Error::io_error)
                        }
                    };
                    #[cfg(any(test, feature = "test_utils"))]
                    if let Some(lat) = read_io_latency {
                        std::thread::sleep(rand::random_range(lat));
                    }
                    (buf, res)
                })
                .await
            {
                Ok((buf, res)) => {
                    #[cfg(feature = "tracing")]
                    drop(span);
                    (buf, res)
                }
                Err(e) => return (Box::new(Raw::new(0)) as Box<dyn IoB>, Err(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::io::engine::psync::write")
    )]
    fn write(&self, buf: Box<dyn IoBuf>, partition: &dyn Partition, offset: u64) -> IoHandle {
        let (raw, offset) = partition.translate(offset);
        let file = FileHandle::from(raw);
        let runtime = self.spawner.clone();

        #[cfg(feature = "tracing")]
        let span = Span::enter_with_local_parent("foyer::storage::io::engine::psync::write::io");

        #[cfg(any(test, feature = "test_utils"))]
        let write_io_latency = self.write_io_latency.clone();
        async move {
            let (buf, res) = match runtime
                .spawn_blocking(move || {
                    let (ptr, len) = buf.as_raw_parts();
                    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
                    let res = {
                        #[cfg(target_family = "windows")]
                        {
                            use std::os::windows::fs::FileExt;
                            file.seek_write(slice, offset).map(|_| ()).map_err(Error::io_error)
                        }
                        #[cfg(target_family = "unix")]
                        {
                            use std::os::unix::fs::FileExt;
                            file.write_all_at(slice, offset).map_err(Error::io_error)
                        }
                    };
                    #[cfg(any(test, feature = "test_utils"))]
                    if let Some(lat) = write_io_latency {
                        std::thread::sleep(rand::random_range(lat));
                    }
                    (buf, res)
                })
                .await
            {
                Ok((buf, res)) => {
                    #[cfg(feature = "tracing")]
                    drop(span);
                    (buf, res)
                }
                Err(e) => return (Box::new(Raw::new(0)) as Box<dyn IoB>, Err(e)),
            };
            let buf: Box<dyn IoB> = buf.into_iob();
            (buf, res)
        }
        .boxed()
        .into()
    }
}
