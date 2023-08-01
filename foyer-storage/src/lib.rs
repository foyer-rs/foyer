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

#![feature(allocator_api)]
#![feature(strict_provenance)]
#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(provide_any)]
#![feature(error_generic_member_access)]
#![allow(clippy::type_complexity)]

pub mod admission;
pub mod device;
pub mod error;
pub mod event;
pub mod flusher;
pub mod indices;
pub mod judge;
pub mod metrics;
pub mod reclaimer;
pub mod region;
pub mod region_manager;
pub mod reinsertion;
pub mod slice;
pub mod store;

pub type LruFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lru::Lru<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lru::LruLink>,
    >,
    foyer_intrusive::eviction::lru::LruLink,
>;

pub type LruFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lru::Lru<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lru::LruLink>,
    >,
>;

pub type LfuFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lfu::Lfu<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lfu::LfuLink>,
    >,
    foyer_intrusive::eviction::lfu::LfuLink,
>;

pub type LfuFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::lfu::Lfu<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::lfu::LfuLink>,
    >,
>;

pub type SegmentedFifoFsStore<K, V> = store::Store<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::sfifo::SegmentedFifo<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::sfifo::SegmentedFifoLink>,
    >,
    foyer_intrusive::eviction::sfifo::SegmentedFifoLink,
>;

pub type SegmentedFifoFsStoreConfig<K, V> = store::StoreConfig<
    K,
    V,
    device::fs::FsDevice,
    foyer_intrusive::eviction::sfifo::SegmentedFifo<
        region_manager::RegionEpItemAdapter<foyer_intrusive::eviction::sfifo::SegmentedFifoLink>,
    >,
>;
