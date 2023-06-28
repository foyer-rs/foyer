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

#![feature(trait_alias)]
#![feature(pattern)]

use std::{fmt::Debug, ptr::NonNull};

use paste::paste;

mod container;
mod metrics;
mod store;

pub trait Weight {
    fn weight(&self) -> usize;
}

impl Weight for Vec<u8> {
    fn weight(&self) -> usize {
        self.len()
    }
}

pub trait Index:
    PartialOrd + Ord + PartialEq + Eq + Clone + std::hash::Hash + Send + Sync + 'static + Debug
{
    fn size() -> usize;

    fn write(&self, buf: &mut [u8]);

    fn read(buf: &[u8]) -> Self;
}
pub trait Data = Send + Sync + 'static + Into<Vec<u8>> + From<Vec<u8>> + Weight;

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro! {
            u8, u16, u32, u64,
            i8, i16, i32, i64,
        }
    };
}

macro_rules! impl_index {
    ($( $type:ty, )*) => {
        use bytes::{Buf, BufMut};

        paste! {
            $(
                impl Index for $type {
                    fn size() -> usize {
                        std::mem::size_of::<$type>()
                    }

                    fn write(&self, mut buf: &mut [u8]) {
                        buf.[< put_ $type>](*self)
                    }

                    fn read(mut buf: &[u8]) -> Self {
                        buf.[< get_ $type>]()
                    }
                }
            )*
        }
    };
}

for_all_primitives! { impl_index }

pub struct WrappedNonNull<T>(pub NonNull<T>);

unsafe impl<T> Send for WrappedNonNull<T> {}
unsafe impl<T> Sync for WrappedNonNull<T> {}

// TODO(MrCroxx): Add global error type.
pub type Error = store::error::Error;

pub type TinyLfuReadOnlyFileStoreCache<I, D> = container::Container<
    I,
    foyer_policy::eviction::tinylfu::TinyLfu<I>,
    foyer_policy::eviction::tinylfu::Handle<I>,
    D,
    store::read_only_file_store::ReadOnlyFileStore<I, D>,
>;

pub type TinyLfuReadOnlyFileStoreCacheConfig<I, D> = container::Config<
    I,
    foyer_policy::eviction::tinylfu::TinyLfu<I>,
    foyer_policy::eviction::tinylfu::Handle<I>,
    store::read_only_file_store::ReadOnlyFileStore<I, D>,
>;

pub type LruReadOnlyFileStoreCache<I, D> = container::Container<
    I,
    foyer_policy::eviction::lru::Lru<I>,
    foyer_policy::eviction::lru::Handle<I>,
    D,
    store::read_only_file_store::ReadOnlyFileStore<I, D>,
>;

pub type LruReadOnlyFileStoreCacheConfig<I, D> = container::Config<
    I,
    foyer_policy::eviction::lru::Lru<I>,
    foyer_policy::eviction::lru::Handle<I>,
    store::read_only_file_store::ReadOnlyFileStore<I, D>,
>;

pub use metrics::Metrics;

pub use foyer_policy::eviction::lru::Config as LruConfig;
pub use foyer_policy::eviction::tinylfu::Config as TinyLfuConfig;
pub use store::read_only_file_store::Config as ReadOnlyFileStoreConfig;

#[cfg(test)]

mod tests {
    pub fn is_send_sync_static<T: Send + Sync + 'static>() {}
}
