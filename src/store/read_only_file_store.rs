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

use std::fs::read_dir;
use std::mem::swap;
use std::path::{Path, PathBuf};

use std::str::pattern::Pattern;
use std::sync::Arc;
use std::{collections::HashMap, marker::PhantomData};

use async_trait::async_trait;

use itertools::Itertools;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{Data, Index};

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

    /// max cache file size
    pub max_file_size: usize,
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

    files: Arc<RwLock<ReadOnlyFileStoreFiles>>,

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

    async fn open(pool: usize, mut config: Self::C) -> Result<Self> {
        config.dir = config.dir.join(format!("{:04}", pool));

        let ids = asyncify({
            let dir = config.dir.clone();
            move || {
                let ids: Vec<FileId> = read_dir(dir)?
                    .map(|entry| entry.unwrap())
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .is_prefix_of(META_FILE_PREFIX)
                    })
                    .map(|entry| {
                        entry.file_name().to_string_lossy()[META_FILE_PREFIX.len()..].to_string()
                    })
                    .map(|s| s.parse().unwrap())
                    .collect_vec();
                Ok(ids)
            }
        })
        .await?;

        let mut frozens = HashMap::new();
        for id in &ids {
            let frozen = Self::open_frozen_file(&config.dir, *id).await?;
            frozens.insert(*id, frozen);
        }
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
            _marker: PhantomData,
        })
    }

    #[allow(clippy::uninit_vec)]
    async fn store(&self, index: Self::I, data: Self::D) -> Result<()> {
        let buf = data.into();

        // append cache file and meta file
        let (fid, sid, location) = {
            let files = self.files.read().await;

            let fid = files.active.fid;

            let location = files.active.cache_file.append(buf).await?;

            let mut buf = Vec::with_capacity(Self::meta_entry_size());
            unsafe { buf.set_len(Self::meta_entry_size()) };
            index.write(&mut buf[..]);
            location.write(&mut buf[I::size()..]);
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

        if active_file_size >= self.config.max_file_size {
            let inner = self.files.write().await;
            // check size again in the critical section to prevent from double rotating
            if inner.active.cache_file.len() >= self.config.max_file_size {
                self.rotate_active_file_locked(inner).await?;
            }
        }

        Ok(())
    }

    async fn load(&self, index: &Self::I) -> Result<Option<Self::D>> {
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
                files
                    .frozens
                    .get(&fid)
                    .expect("frozen file not found")
                    .cache_file
                    .read(location.offset as u64, location.len as usize)
                    .await?
            }
        };

        Ok(Some(buf.into()))
    }

    async fn delete(&self, index: &Self::I) -> Result<()> {
        let (fid, sid, _location) = {
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
                files
                    .frozens
                    .get(&fid)
                    .expect("frozen file not found")
                    .meta_file
                    .write(sid as u64 * Self::meta_entry_size() as u64, empty_entry)
                    .await?;
            }
        }

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

        Ok(())
    }

    pub async fn reclaim_frozen_file(&self, id: FileId) -> Result<()> {
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

        {
            let mut indices = self.indices.write().await;
            for index in indices_to_delete.keys() {
                indices.remove(index);
            }
        }

        meta_file.reclaim().await?;
        cache_file.reclaim().await?;

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
            let index = I::read(slice);
            let location = Location::read(&slice[I::size()..]);
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
        I::size() + Location::size()
    }
}
