//  Copyright 2024 foyer Project Authors
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

use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut};
use foyer_common::asyncify::asyncify_with_runtime;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;

use crate::{
    error::{Error, Result},
    large::serde::Sequence,
    runtime::Runtime,
    serde::Checksummer,
};

/// Persistent metadata for the disk cache.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// watermark for large object disk cache.
    pub sequence_watermark: Sequence,
    /// watermark for small object disk cache
    pub timestamp_watermark: u128,
}

impl Metadata {
    /// | magic 8B | checksum 8B | sequence watermark 8B | timestamp watermark 16B |
    pub const LENGTH: usize = 8 + 8 + 8 + 16;
    /// | magic 8B | checksum 8B |
    pub const HEADER: usize = 16;
    /// magic number for metadata
    pub const MAGIC: u64 = 0x20230512deadbeef;

    pub fn new(sequence: Sequence, timestamp: u128) -> Self {
        Self {
            sequence_watermark: sequence,
            timestamp_watermark: timestamp,
        }
    }

    pub fn read(buf: &[u8], default: Self) -> Self {
        let magic = (&buf[0..8]).get_u64();
        let checksum = (&buf[8..16]).get_u64();
        let sequence_watermark = (&buf[16..24]).get_u64();
        let timestamp_watermark = (&buf[24..40]).get_u128();

        let c = Checksummer::checksum64(&buf[Self::HEADER..Self::LENGTH]);

        if magic != Self::MAGIC || checksum != c {
            tracing::warn!(
                "[manifest]: manifest magic or checksum mismatch, update the watermark to the current timestamp."
            );
            return default;
        }

        Self {
            sequence_watermark,
            timestamp_watermark,
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        (&mut buf[16..24]).put_u64(self.sequence_watermark);
        (&mut buf[24..40]).put_u128(self.timestamp_watermark);

        let checksum = Checksummer::checksum64(&buf[Self::HEADER..Self::LENGTH]);

        (&mut buf[0..8]).put_u64(Self::MAGIC);
        (&mut buf[8..16]).put_u64(checksum);
    }

    pub fn timestamp() -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    }
}

/// Manifest file for persistent metadata for the disk cache.
#[derive(Debug, Clone)]
pub struct Manifest {
    inner: Arc<AsyncRwLock<ManifestInner>>,
}

#[derive(Debug)]
struct ManifestInner {
    metadata: Metadata,

    file: Arc<File>,

    flush: bool,
    runtime: Runtime,
}

impl Manifest {
    /// Default manifest filename
    pub const DEFAULT_FILENAME: &str = "manifest";

    pub async fn open<P>(path: P, flush: bool, runtime: Runtime) -> Result<Self>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let file = asyncify_with_runtime(runtime.user(), move || {
            let mut opts = OpenOptions::new();
            opts.create(true).read(true).write(true);
            opts.open(path)
        })
        .await?;
        let file = Arc::new(file);

        let f = file.clone();
        let metadata = asyncify_with_runtime(runtime.read(), move || {
            let mut buf = [0; Metadata::LENGTH];

            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                let _ = f.seek_read(&mut buf[..], 0);
            };

            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                let _ = f.read_exact_at(&mut buf[..], 0);
            };

            // If the metadata is corrupted, the watermark is supposed to set as the current timestamp to prevent from
            // accessing stale data.
            let metadata = Metadata::read(&buf[..], Metadata::new(u64::MAX, Metadata::timestamp()));
            Ok::<_, Error>(metadata)
        })
        .await?;

        let inner = ManifestInner {
            metadata,
            file,
            flush,
            runtime,
        };

        Ok(Self {
            inner: Arc::new(AsyncRwLock::new(inner)),
        })
    }

    pub async fn sequence_watermark(&self) -> Sequence {
        self.inner.read().await.metadata.sequence_watermark
    }

    pub async fn timestamp_watermark(&self) -> u128 {
        self.inner.read().await.metadata.timestamp_watermark
    }

    /// Update watermark and flush.
    pub async fn update_timestamp_watermark(&self, watermark: u128) -> Result<()> {
        let mut inner = self.inner.write().await;

        inner.metadata.timestamp_watermark = watermark;

        let mut buf = [0; Metadata::LENGTH];
        inner.metadata.write(&mut buf[..]);

        let file = inner.file.clone();
        let flush = inner.flush;
        asyncify_with_runtime(inner.runtime.write(), move || {
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                f.seek_write(&buf[..], 0)?;
            };

            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&buf[..], 0)?;
            };

            if flush {
                file.sync_data()?;
            }
            Ok::<_, Error>(())
        })
        .await?;

        drop(inner);

        Ok(())
    }

    /// Update watermark and flush.
    pub async fn update_sequence_watermark(&self, watermark: Sequence) -> Result<()> {
        let mut inner = self.inner.write().await;

        inner.metadata.sequence_watermark = watermark;

        let mut buf = [0; Metadata::LENGTH];
        inner.metadata.write(&mut buf[..]);

        let file = inner.file.clone();
        let flush = inner.flush;
        asyncify_with_runtime(inner.runtime.write(), move || {
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                file.seek_write(&buf[..], 0)?;
            };

            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&buf[..], 0)?;
            };

            if flush {
                file.sync_data()?;
            }
            Ok::<_, Error>(())
        })
        .await?;

        drop(inner);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test_log::test]
    fn test_metadata_serde() {
        let mut buf = [0; Metadata::LENGTH];
        let mut metadata = Metadata::read(&buf[..], Metadata::new(114, 514));
        assert_eq!(metadata.sequence_watermark, 114);
        assert_eq!(metadata.timestamp_watermark, 514);

        metadata.timestamp_watermark = 0x0123456789abcdef;
        metadata.write(&mut buf[..]);

        let m = Metadata::read(&buf[..], Metadata::new(114, 514));
        assert_eq!(metadata, m);
    }

    #[test_log::test(tokio::test)]
    async fn test_manifest_file() {
        let dir = tempdir().unwrap();

        let manifest = Manifest::open(dir.path().join("manifest"), true, Runtime::current())
            .await
            .unwrap();

        let w = Metadata::timestamp();

        manifest.update_timestamp_watermark(w).await.unwrap();

        let manifest = Manifest::open(dir.path().join("manifest"), true, Runtime::current())
            .await
            .unwrap();

        let watermark = manifest.timestamp_watermark().await;

        assert_eq!(watermark, w);
    }
}
