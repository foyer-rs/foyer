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

use std::{fmt::Debug, fs::File, mem::ManuallyDrop, os::fd::FromRawFd, sync::Arc};

use futures_util::FutureExt;

use crate::io::{
    bytes::{IoB, IoBuf, IoBufMut},
    device::{Device, RegionId},
    engine::{IoEngine, IoEngineBuilder, IoHandle},
    error::{IoError, IoResult},
};

#[derive(Debug)]
pub struct PsyncIoEngineBuilder;

impl PsyncIoEngineBuilder {
    pub fn new() -> Self {
        Self
    }
}

impl IoEngineBuilder for PsyncIoEngineBuilder {
    fn build(self: Box<Self>, device: Arc<dyn Device>) -> IoResult<Arc<dyn IoEngine>> {
        let engine = PsyncIoEngine { device: device };
        let engine = Arc::new(engine);
        Ok(engine)
    }
}

struct RawBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for RawBuf {}
unsafe impl Sync for RawBuf {}

pub struct PsyncIoEngine {
    device: Arc<dyn Device>,
}

impl Debug for PsyncIoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PsyncIoEngine").finish()
    }
}

impl IoEngine for PsyncIoEngine {
    fn device(&self) -> &Arc<dyn Device> {
        &self.device
    }

    fn read(&self, buf: Box<dyn IoBufMut>, region: RegionId, offset: u64) -> IoHandle {
        let device = self.device.clone();
        let (ptr, len) = buf.as_raw_parts();
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        async move {
            let res = match tokio::task::spawn_blocking(move || {
                let (fd, offset) = device.translate(region, offset);
                let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
                #[cfg(target_family = "windows")]
                {
                    use std::os::windows::fs::FileExt;
                    file.seek_read(slice, offset).map_err(IoError::from)?;
                };
                #[cfg(target_family = "unix")]
                {
                    use std::os::unix::fs::FileExt;
                    file.read_exact_at(slice, offset).map_err(IoError::from)?;
                };
                Ok(())
            })
            .await
            {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf;
            (buf, res)
        }
        .boxed()
        .into()
    }

    fn write(&self, buf: Box<dyn IoBuf>, region: RegionId, offset: u64) -> IoHandle {
        let device = self.device.clone();
        let (ptr, len) = buf.as_raw_parts();
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        async move {
            let res = match tokio::task::spawn_blocking(move || {
                let (fd, offset) = device.translate(region, offset);
                let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
                #[cfg(target_family = "windows")]
                {
                    use std::os::windows::fs::FileExt;
                    file.seek_write(slice, offset).map_err(IoError::from)?;
                };
                #[cfg(target_family = "unix")]
                {
                    use std::os::unix::fs::FileExt;
                    file.write_all_at(slice, offset).map_err(IoError::from)?;
                };
                Ok(())
            })
            .await
            {
                Ok(res) => res,
                Err(e) => Err(IoError::other(e)),
            };
            let buf: Box<dyn IoB> = buf;
            (buf, res)
        }
        .boxed()
        .into()
    }
}
