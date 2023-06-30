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

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use bytes::{Buf, BufMut};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use foyer_utils::{bits::align_up, queue::AsyncQueue};
use twox_hash::XxHash64;

use crate::{
    admission::AdmissionPolicy,
    device::{BufferAllocator, Device},
    error::{Error, Result},
    flusher::Flusher,
    indices::{Index, Indices},
    reclaimer::Reclaimer,
    region::RegionId,
    region_manager::{RegionEpItemAdapter, RegionManager},
    reinsertion::ReinsertionPolicy,
};
use foyer_common::{Key, Value};
use std::hash::Hasher;

pub struct StoreConfig<D, AP, EP, RP, EL>
where
    D: Device,
    AP: AdmissionPolicy,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    RP: ReinsertionPolicy,
    EL: Link,
{
    pub eviction_config: EP::Config,
    pub device_config: D::Config,
    pub admission: AP,
    pub reinsertion: RP,
    pub buffer_pool_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
}

impl<D, AP, EP, RP, EL> Debug for StoreConfig<D, AP, EP, RP, EL>
where
    D: Device,
    AP: AdmissionPolicy,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    RP: ReinsertionPolicy,
    EL: Link,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreConfig")
            .field("eviction_config", &self.eviction_config)
            .field("device_config", &self.device_config)
            .field("admission", &self.admission)
            .field("reinsertion", &self.reinsertion)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .finish()
    }
}

pub struct Store<K, V, BA, D, EP, AP, RP, EL>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    AP: AdmissionPolicy<Key = K, Value = V>,
    RP: ReinsertionPolicy<Key = K, Value = V>,
    EL: Link,
{
    indices: Arc<Indices<K>>,

    region_manager: Arc<RegionManager<BA, D, EP, EL>>,

    device: D,

    admission: AP,

    _marker: PhantomData<(V, RP)>,
}

impl<K, V, BA, D, EP, AP, RP, EL> Store<K, V, BA, D, EP, AP, RP, EL>
where
    K: Key,
    V: Value,
    BA: BufferAllocator,
    D: Device<IoBufferAllocator = BA>,
    EP: EvictionPolicy<RegionEpItemAdapter<EL>, Link = EL>,
    AP: AdmissionPolicy<Key = K, Value = V>,
    RP: ReinsertionPolicy<Key = K, Value = V>,
    EL: Link,
{
    pub async fn open(config: StoreConfig<D, AP, EP, RP, EL>) -> Result<Arc<Self>> {
        let device = D::open(config.device_config).await?;

        let buffers = Arc::new(AsyncQueue::new());
        for _ in 0..(config.buffer_pool_size / device.region_size()) {
            let len = device.region_size();
            let buffer = device.io_buffer(len, len);
            buffers.release(buffer);
        }

        let clean_regions = Arc::new(AsyncQueue::new());
        for region_id in 0..device.regions() as RegionId {
            clean_regions.release(region_id);
        }

        let flusher = Arc::new(Flusher::new(config.flushers));
        let reclaimer = Arc::new(Reclaimer::new(config.reclaimers));

        let region_manager = Arc::new(RegionManager::new(
            device.regions(),
            config.eviction_config,
            buffers.clone(),
            clean_regions.clone(),
            device.clone(),
            flusher.clone(),
            reclaimer.clone(),
        ));

        let indices = Arc::new(Indices::new(device.regions()));

        let store = Arc::new(Self {
            indices: indices.clone(),
            region_manager: region_manager.clone(),
            device,
            admission: config.admission,
            _marker: PhantomData,
        });

        flusher.run(buffers, region_manager.clone()).await;
        reclaimer
            .run(
                store.clone(),
                region_manager,
                clean_regions,
                config.reinsertion,
                indices,
            )
            .await;

        Ok(store)
    }

    pub async fn insert(&self, key: K, value: V) -> Result<bool> {
        if !self.admission.judge(&key, &value) {
            return Ok(false);
        }

        let serialized_len = self.serialized_len(&key, &value);

        let mut slice = self.region_manager.allocate(serialized_len).await;

        let mut offset = 0;
        value.write(&mut slice.as_mut()[offset..offset + value.serialized_len()]);
        offset += value.serialized_len();
        key.write(&mut slice.as_mut()[offset..offset + key.serialized_len()]);
        offset += key.serialized_len();

        let checksum = checksum(&slice.as_ref()[..offset]);

        let footer = Footer {
            key_len: key.serialized_len() as u32,
            value_len: value.serialized_len() as u32,
            checksum,
        };
        offset = slice.len() - Footer::serialized_len();
        footer.write(&mut slice.as_mut()[offset..]);

        let index = Index {
            region: slice.region_id(),
            version: slice.version(),
            offset: slice.offset() as u32,
            len: slice.len() as u32,
            key_len: key.serialized_len() as u32,
            value_len: value.serialized_len() as u32,

            key,
        };

        drop(slice);

        self.indices.insert(index);

        Ok(true)
    }

    pub async fn lookup(&self, key: &K) -> Result<Option<V>> {
        let index = match self.indices.lookup(key) {
            Some(index) => index,
            None => return Ok(None),
        };

        self.region_manager.record_access(&index.region).await;
        let region = self.region_manager.region(&index.region);
        let start = index.offset as usize;
        // TODO(MrCroxx): read value and checksum only
        let end = start + index.len as usize;
        let slice = match region.load(start..end, index.version).await? {
            Some(slice) => slice,
            None => return Ok(None),
        };

        let checksum = checksum(&slice.as_ref()[..(index.value_len + index.key_len) as usize]);
        let expected = (&slice.as_ref()[slice.len() - 8..]).get_u64();
        if checksum != expected {
            return Err(Error::ChecksumMismatch { checksum, expected });
        }

        let value = V::read(&slice.as_ref()[..index.value_len as usize]);

        Ok(Some(value))
    }

    pub fn remove(&self, key: &K) {
        self.indices.remove(key);
    }

    fn serialized_len(&self, key: &K, value: &V) -> usize {
        let unaligned = key.serialized_len() + value.serialized_len() + Footer::serialized_len();
        align_up(self.device.align(), unaligned)
    }
}

struct Footer {
    key_len: u32,
    value_len: u32,
    checksum: u64,
}

impl Footer {
    fn serialized_len() -> usize {
        4 + 4 + 8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.checksum);
    }

    #[allow(dead_code)]
    fn read(mut buf: &[u8]) -> Self {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let checksum = buf.get_u64();

        Self {
            key_len,
            value_len,
            checksum,
        }
    }
}

fn checksum(buf: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(buf);
    hasher.finish()
}
