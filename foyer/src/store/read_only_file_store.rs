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

use std::fs::{create_dir_all, read_dir};
use std::mem::swap;
use std::path::{Path, PathBuf};

use std::str::pattern::Pattern;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, marker::PhantomData};

use async_trait::async_trait;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use rand::{thread_rng, Rng};
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{Data, Index, Metrics};

use super::error::Result;
use super::file::{AppendableFile, Location, ReadableFile, WritableFile};
use super::{asyncify, Store};

pub type FileId = u32;
pub type SlotId = u32;

const META_FILE_PREFIX: &str = "metafile-";
const CACHE_FILE_PREFIX: &str = "cachefile-";

#[derive(Clone, Debug)]
pub struct Config {
    /// path to store dir
    pub dir: PathBuf,

    /// store capacity
    pub capacity: usize,

    /// max cache file size
    pub max_file_size: usize,

    /// ratio of garbage to trigger reclaim
    pub trigger_reclaim_garbage_ratio: f64,

    /// ratio of size to trigger reclaim
    pub trigger_reclaim_capacity_ratio: f64,

    /// ratio of size to trigger randomly drop
    pub trigger_random_drop_ratio: f64,

    /// ratio of randomly dropped entries
    pub random_drop_ratio: f64,
}

struct Frozen {
    fid: FileId,
    meta_file: WritableFile,
    cache_file: ReadableFile,
}

struct Active {
    fid: FileId,
    meta_file: WritableFile,
    cache_file: AppendableFile,
}

struct ReadOnlyFileStoreFiles {
    active: Active,

    frozens: HashMap<FileId, Frozen>,
}

/// `ReadOnlyFileStore` is a file store for read only entries.
///
/// The cache data for a cache key MUST NOT be updated.
#[allow(clippy::type_complexity)]
pub struct ReadOnlyFileStore<I, D>
where
    I: Index,
    D: Data,
{
    pool: usize,

    config: Arc<Config>,

    indices: Arc<RwLock<HashMap<I, (FileId, SlotId, Location)>>>,

    /// write lock is used when rotating active file or reclaiming frozen files
    files: Arc<RwLock<ReadOnlyFileStoreFiles>>,

    size: Arc<AtomicUsize>,

    metrics: Arc<Metrics>,

    _marker: PhantomData<D>,
}

impl<I, D> Clone for ReadOnlyFileStore<I, D>
where
    I: Index,
    D: Data,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool,
            config: Arc::clone(&self.config),
            indices: Arc::clone(&self.indices),
            files: Arc::clone(&self.files),
            size: Arc::clone(&self.size),
            metrics: Arc::clone(&self.metrics),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<I, D> Store for ReadOnlyFileStore<I, D>
where
    I: Index,
    D: Data,
{
    type I = I;

    type D = D;

    type C = Config;

    async fn open(pool: usize, mut config: Self::C, metrics: Arc<Metrics>) -> Result<Self> {
        config.dir = config.dir.join(format!("{:04}", pool));

        let ids = asyncify({
            let dir = config.dir.clone();
            move || {
                create_dir_all(&dir)?;

                let mut ids: Vec<FileId> = read_dir(dir)?
                    .map(|entry| entry.unwrap())
                    .filter(|entry| {
                        META_FILE_PREFIX.is_prefix_of(&entry.file_name().to_string_lossy())
                    })
                    .map(|entry| {
                        entry.file_name().to_string_lossy()[META_FILE_PREFIX.len()..].to_string()
                    })
                    .map(|s| s.parse().unwrap())
                    .collect_vec();
                ids.sort();
                Ok(ids)
            }
        })
        .await?;

        let mut frozens = HashMap::new();
        let mut size = 0;
        for id in &ids {
            let frozen = Self::open_frozen_file(&config.dir, *id).await?;
            // when restore, `len` is filled with `fstat(2)`
            size += frozen.cache_file.len();
            frozens.insert(*id, frozen);
        }
        // create new active file on every `open`
        let id = ids.into_iter().max().unwrap_or(0) + 1;
        let active = Self::open_active_file(&config.dir, id).await?;

        let files = ReadOnlyFileStoreFiles { active, frozens };
        let mut indices = HashMap::new();
        for (fid, frozen) in &files.frozens {
            Self::restore_meta(*fid, &frozen.meta_file, &mut indices).await?;
        }

        Ok(Self {
            pool,
            config: Arc::new(config),
            indices: Arc::new(RwLock::new(indices)),
            files: Arc::new(RwLock::new(files)),
            size: Arc::new(AtomicUsize::new(size)),
            metrics,
            _marker: PhantomData,
        })
    }

    #[allow(clippy::uninit_vec)]
    async fn store(&self, index: Self::I, data: Self::D) -> Result<()> {
        let _timer = self.metrics.latency_store.start_timer();

        // append cache file and meta file
        let (fid, sid, location) = {
            // randomly drop if size exceeds the threshold
            if self.size.load(Ordering::Relaxed) as f64
                >= self.config.capacity as f64 * self.config.trigger_random_drop_ratio
            {
                let mut rng = thread_rng();
                if rng.gen_range(0.0..1.0) < self.config.random_drop_ratio {
                    return Ok(());
                }
            }

            let buf = data.into();

            let files = self.files.read().await;

            let fid = files.active.fid;

            let location = files.active.cache_file.append(buf).await?;

            let mut buf = Vec::with_capacity(Self::meta_entry_size());

            let tags = Tags { valid: true };

            unsafe { buf.set_len(Self::meta_entry_size()) };
            tags.write(&mut buf[..]);
            index.write(&mut buf[Tags::size()..]);
            location.write(&mut buf[Tags::size() + I::size()..]);

            let Location {
                offset: meta_offset,
                len: _,
            } = files.active.meta_file.append(buf).await?;
            let sid = meta_offset / Self::meta_entry_size() as u32;

            (fid, sid, location)
        };

        let active_file_size = (location.offset + location.len) as usize;

        {
            let mut indices = self.indices.write().await;
            indices.insert(index, (fid, sid, location));
            drop(indices);
        }

        let cache_data_size = self
            .size
            .fetch_add(location.len as usize, Ordering::Relaxed)
            + location.len as usize;

        self.metrics.bytes_store.inc_by(location.len as f64);
        self.metrics.cache_data_size.set(cache_data_size as f64);

        if active_file_size >= self.config.max_file_size {
            let files = self.files.write().await;
            // check size again in the critical section to prevent from double rotating
            if files.active.cache_file.len() >= self.config.max_file_size {
                self.rotate_active_file_locked(files).await?;
            }
        }

        self.maybe_trigger_reclaim().await?;

        Ok(())
    }

    async fn load(&self, index: &Self::I) -> Result<Option<Self::D>> {
        let _timer = self.metrics.latency_load.start_timer();

        // TODO(MrCroxx): add bloom filters ?
        let (fid, _sid, location) = {
            let indices = self.indices.read().await;

            let (fid, sid, location) = match indices.get(index) {
                Some((fid, sid, location)) => (*fid, *sid, *location),
                None => return Ok(None),
            };

            (fid, sid, location)
        };

        let buf = {
            let files = self.files.read().await;

            if fid == files.active.fid {
                files
                    .active
                    .cache_file
                    .read(location.offset as u64, location.len as usize)
                    .await?
            } else {
                match files.frozens.get(&fid) {
                    Some(frozen) => {
                        frozen
                            .cache_file
                            .read(location.offset as u64, location.len as usize)
                            .await?
                    }
                    None => {
                        tracing::error!("frozen file {} not found", fid);
                        return Ok(None);
                    }
                }
            }
        };

        self.metrics.bytes_load.inc_by(location.len as f64);

        self.maybe_trigger_reclaim().await?;

        Ok(Some(buf.into()))
    }

    async fn delete(&self, index: &Self::I) -> Result<()> {
        let _timer = self.metrics.latency_delete.start_timer();

        let (fid, sid, location) = {
            let indices = self.indices.read().await;
            let (fid, sid, location) = match indices.get(index) {
                Some((fid, sid, location)) => (*fid, *sid, *location),
                None => return Ok(()),
            };
            (fid, sid, location)
        };

        {
            let empty_entry = vec![0; Self::meta_entry_size()];
            let files = self.files.read().await;
            if fid == files.active.fid {
                files
                    .active
                    .meta_file
                    .write(sid as u64 * Self::meta_entry_size() as u64, empty_entry)
                    .await?;
            } else {
                match files.frozens.get(&fid) {
                    Some(frozen) => {
                        frozen
                            .meta_file
                            .write(sid as u64 * Self::meta_entry_size() as u64, empty_entry)
                            .await?;
                    }
                    None => {
                        tracing::error!("frozen file {} not found", fid);
                    }
                }
            }
        }

        let cache_data_size = self
            .size
            .fetch_sub(location.len as usize, Ordering::Relaxed)
            - location.len as usize;

        self.metrics.bytes_delete.inc_by(location.len as f64);
        self.metrics.cache_data_size.set(cache_data_size as f64);

        self.maybe_trigger_reclaim().await?;

        Ok(())
    }
}

impl<I, D> ReadOnlyFileStore<I, D>
where
    I: Index,
    D: Data,
{
    async fn rotate_active_file_locked<'a>(
        &self,
        mut files: RwLockWriteGuard<'a, ReadOnlyFileStoreFiles>,
    ) -> Result<()> {
        // rotate active file
        let mut active = Self::open_active_file(&self.config.dir, files.active.fid + 1).await?;
        swap(&mut active, &mut files.active);

        // open frozen file
        let id = active.fid;
        let frozen = Self::open_frozen_file(&self.config.dir, id).await?;

        // update frozen map
        files.frozens.insert(id, frozen);

        tracing::info!("active file rotated: {} => {}", id, id + 1);

        Ok(())
    }

    async fn reclaim_frozen_file(&self, id: FileId) -> Result<()> {
        tracing::info!("reclaiming frozen file {}", id);

        let (fid, meta_file, cache_file) = {
            let mut files = self.files.write().await;

            let Frozen {
                fid,
                meta_file,
                cache_file,
            } = files.frozens.remove(&id).expect("frozen id not exists");

            (fid, meta_file, cache_file)
        };

        let mut indices_to_delete = HashMap::new();
        Self::restore_meta(fid, &meta_file, &mut indices_to_delete).await?;

        let size = {
            let mut size = 0;
            let mut indices = self.indices.write().await;
            for (index, (_fid, _sid, Location { offset: _, len })) in indices_to_delete {
                indices.remove(&index);
                size += len;
            }
            size as usize
        };

        let cache_data_size = self.size.fetch_sub(size, Ordering::Relaxed) - size;
        self.metrics.cache_data_size.set(cache_data_size as f64);

        meta_file.reclaim().await?;
        cache_file.reclaim().await?;

        tracing::info!("frozen file {} reclaimed", id);

        Ok(())
    }

    async fn maybe_trigger_reclaim(&self) -> Result<()> {
        // trigger by size ratio
        if self.size.load(Ordering::Relaxed) as f64
            >= self.config.capacity as f64 * self.config.trigger_reclaim_capacity_ratio
        {
            self.reclaim().await?;
        }

        // TODO(MrCroxx): trigger reclaim based on garbage ratio

        Ok(())
    }

    /// Reclaim garbage to make room.
    ///
    /// Policy:
    ///
    /// [WIP] For now, simply reclaim the oldest frozen file.
    ///
    /// TODO(MrCroxx): better reclaim policy
    async fn reclaim(&self) -> Result<()> {
        let id = {
            let files = self.files.read().await;
            let id = files.frozens.keys().min();
            match id {
                Some(id) => *id,
                None => return Ok(()),
            }
        };

        self.reclaim_frozen_file(id).await?;

        Ok(())
    }

    async fn open_active_file(dir: impl AsRef<Path>, id: FileId) -> Result<Active> {
        let meta_file = WritableFile::open(Self::meta_file_path(&dir, id)).await?;
        let cache_file = AppendableFile::open(Self::cache_file_path(&dir, id)).await?;
        Ok(Active {
            fid: id,
            meta_file,
            cache_file,
        })
    }

    async fn open_frozen_file(dir: impl AsRef<Path>, id: FileId) -> Result<Frozen> {
        let meta_file = WritableFile::open(Self::meta_file_path(&dir, id)).await?;
        let cache_file = ReadableFile::open(Self::cache_file_path(&dir, id)).await?;

        Ok(Frozen {
            fid: id,
            meta_file,
            cache_file,
        })
    }

    async fn restore_meta(
        fid: FileId,
        meta: &WritableFile,
        indices: &mut HashMap<I, (FileId, SlotId, Location)>,
    ) -> Result<()> {
        let size = meta.size().await?;
        let slots = size / Self::meta_entry_size();
        let buf = meta.read(0, size).await?;
        for sid in 0..slots {
            let slice = &buf[sid * Self::meta_entry_size()..(sid + 1) * Self::meta_entry_size()];
            let tags = Tags::read(slice);
            if !tags.valid {
                continue;
            }
            let index = I::read(&slice[Tags::size()..]);
            let location = Location::read(&slice[Tags::size() + I::size()..]);
            indices.insert(index, (fid, sid as SlotId, location));
        }
        Ok(())
    }

    fn cache_file_path(dir: impl AsRef<Path>, id: FileId) -> PathBuf {
        PathBuf::from(dir.as_ref()).join(format!("{}{:08}", META_FILE_PREFIX, id))
    }

    fn meta_file_path(dir: impl AsRef<Path>, id: FileId) -> PathBuf {
        PathBuf::from(dir.as_ref()).join(format!("{}{:08}", CACHE_FILE_PREFIX, id))
    }

    fn meta_entry_size() -> usize {
        I::size() + Location::size() + Tags::size()
    }
}

struct Tags {
    valid: bool,
}

impl Tags {
    fn size() -> usize {
        1
    }

    fn write(&self, mut buf: &mut [u8]) {
        let mut val = 0;
        val |= self.valid as u8;
        buf.put_u8(val);
    }

    fn read(mut buf: &[u8]) -> Self {
        let val = buf.get_u8();
        let valid = (val & 1) != 0;
        Self { valid }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn data(i: u8, len: usize) -> Vec<u8> {
        vec![i; len]
    }

    #[tokio::test]
    async fn test_read_only_file_store_simple() {
        let dir = tempdir().unwrap();

        let config = Config {
            dir: dir.path().to_owned(),
            max_file_size: 4 * 1024,
            capacity: 16 * 1024,
            trigger_reclaim_garbage_ratio: 0.0, // disabled
            trigger_reclaim_capacity_ratio: 0.75,
            trigger_random_drop_ratio: 0.0, // disabled
            random_drop_ratio: 0.0,         // disabled
        };

        let store: ReadOnlyFileStore<u64, Vec<u8>> =
            ReadOnlyFileStore::open(0, config, Arc::new(Metrics::default()))
                .await
                .unwrap();

        store.store(1, data(1, 1024)).await.unwrap();
        assert_eq!(store.load(&1).await.unwrap(), Some(data(1, 1024)));

        store.store(2, data(2, 1024)).await.unwrap();
        assert_eq!(store.load(&2).await.unwrap(), Some(data(2, 1024)));
        store.store(3, data(3, 1024)).await.unwrap();
        assert_eq!(store.load(&3).await.unwrap(), Some(data(3, 1024)));
        store.store(4, data(4, 1024)).await.unwrap();
        assert_eq!(store.load(&4).await.unwrap(), Some(data(4, 1024)));

        // assert rotate
        assert_eq!(store.files.read().await.frozens.len(), 1);

        assert_eq!(store.load(&1).await.unwrap(), Some(data(1, 1024)));
        assert_eq!(store.load(&2).await.unwrap(), Some(data(2, 1024)));
        assert_eq!(store.load(&3).await.unwrap(), Some(data(3, 1024)));
        assert_eq!(store.load(&4).await.unwrap(), Some(data(4, 1024)));

        store.store(5, data(5, 4 * 1024)).await.unwrap();
        assert_eq!(store.size.load(Ordering::Relaxed), 8 * 1024);
        assert_eq!(store.files.read().await.frozens.len(), 2);

        // assert reclaim
        store.store(6, data(6, 4 * 1024)).await.unwrap();
        assert_eq!(store.files.read().await.frozens.len(), 2);
        assert_eq!(store.size.load(Ordering::Relaxed), 8 * 1024);

        assert_eq!(store.load(&1).await.unwrap(), None);
        assert_eq!(store.load(&2).await.unwrap(), None);
        assert_eq!(store.load(&3).await.unwrap(), None);
        assert_eq!(store.load(&4).await.unwrap(), None);

        drop(dir);
    }

    #[tokio::test]
    async fn test_read_only_file_store_recovery() {
        let dir = tempdir().unwrap();

        let config = Config {
            dir: dir.path().to_owned(),
            max_file_size: 4 * 1024,
            capacity: 16 * 1024,
            trigger_reclaim_garbage_ratio: 0.0, // disabled
            trigger_reclaim_capacity_ratio: 0.75,
            trigger_random_drop_ratio: 0.0, // disabled
            random_drop_ratio: 0.0,         // disabled
        };

        let store: ReadOnlyFileStore<u64, Vec<u8>> =
            ReadOnlyFileStore::open(0, config.clone(), Arc::new(Metrics::default()))
                .await
                .unwrap();

        for i in 0..20 {
            store.store(i, data(i as u8, 1024)).await.unwrap();
        }

        assert_eq!(store.files.read().await.frozens.len(), 2);
        for i in 0..12 {
            assert_eq!(store.load(&i).await.unwrap(), None);
        }
        for i in 12..20 {
            assert_eq!(store.load(&i).await.unwrap(), Some(data(i as u8, 1024)));
        }

        drop(store);

        let store: ReadOnlyFileStore<u64, Vec<u8>> =
            ReadOnlyFileStore::open(0, config, Arc::new(Metrics::default()))
                .await
                .unwrap();

        assert_eq!(store.files.read().await.frozens.len(), 3);
        for i in 0..12 {
            assert_eq!(store.load(&i).await.unwrap(), None);
        }
        for i in 12..20 {
            assert_eq!(store.load(&i).await.unwrap(), Some(data(i as u8, 1024)));
        }

        drop(dir);
    }
}
