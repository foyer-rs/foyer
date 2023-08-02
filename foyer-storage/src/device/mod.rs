//  Copyright 2023 MrCroxx
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

pub mod allocator;
pub mod error;
pub mod fs;

use async_trait::async_trait;
use std::{alloc::Allocator, fmt::Debug};

use crate::region::RegionId;
use error::DeviceResult;

pub trait BufferAllocator = Allocator + Clone + Send + Sync + 'static + Debug;
pub trait IoBuf = AsRef<[u8]> + Send + Sync + 'static + Debug;
pub trait IoBufMut = AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static + Debug;

#[async_trait]
pub trait Device: Sized + Clone + Send + Sync + 'static + Debug {
    type IoBufferAllocator: BufferAllocator;
    type Config: Debug;

    async fn open(config: Self::Config) -> DeviceResult<Self>;

    async fn write(
        &self,
        buf: impl IoBuf,
        region: RegionId,
        offset: u64,
        len: usize,
    ) -> DeviceResult<usize>;

    async fn read(
        &self,
        buf: impl IoBufMut,
        region: RegionId,
        offset: u64,
        len: usize,
    ) -> DeviceResult<usize>;

    async fn flush(&self) -> DeviceResult<()>;

    fn capacity(&self) -> usize;

    fn regions(&self) -> usize;

    fn align(&self) -> usize;

    fn io_size(&self) -> usize;

    fn io_buffer_allocator(&self) -> &Self::IoBufferAllocator;

    fn io_buffer(&self, len: usize, capacity: usize) -> Vec<u8, Self::IoBufferAllocator>;

    fn region_size(&self) -> usize {
        debug_assert!(self.capacity() % self.regions() == 0);
        self.capacity() / self.regions()
    }
}

#[tracing::instrument(level = "trace", skip(f))]
async fn asyncify<F, T>(f: F) -> DeviceResult<T>
where
    F: FnOnce() -> DeviceResult<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(format!("background task failed: {:?}", e,).into()),
    }
}

#[cfg(test)]
pub mod tests {
    use super::{allocator::AlignedAllocator, *};

    #[derive(Debug, Clone)]
    pub struct NullDevice(AlignedAllocator);

    impl NullDevice {
        pub fn new(align: usize) -> Self {
            Self(AlignedAllocator::new(align))
        }
    }

    #[async_trait]
    impl Device for NullDevice {
        type Config = usize;
        type IoBufferAllocator = AlignedAllocator;

        async fn open(config: usize) -> DeviceResult<Self> {
            Ok(Self::new(config))
        }

        async fn write(
            &self,
            _buf: impl IoBuf,
            _region: RegionId,
            _offset: u64,
            _len: usize,
        ) -> DeviceResult<usize> {
            Ok(0)
        }

        async fn read(
            &self,
            _buf: impl IoBufMut,
            _region: RegionId,
            _offset: u64,
            _len: usize,
        ) -> DeviceResult<usize> {
            Ok(0)
        }

        async fn flush(&self) -> DeviceResult<()> {
            Ok(())
        }

        fn capacity(&self) -> usize {
            usize::MAX
        }

        fn regions(&self) -> usize {
            4096
        }

        fn align(&self) -> usize {
            4096
        }

        fn io_size(&self) -> usize {
            4096
        }

        fn io_buffer_allocator(&self) -> &Self::IoBufferAllocator {
            &self.0
        }

        fn io_buffer(&self, len: usize, capacity: usize) -> Vec<u8, Self::IoBufferAllocator> {
            let mut buf = Vec::with_capacity_in(capacity, self.0);
            unsafe { buf.set_len(len) };
            buf
        }
    }
}
