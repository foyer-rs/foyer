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

use std::{hash::BuildHasherDefault, num::NonZeroUsize, sync::Arc};

use anyhow::Result;
use chrono::Datelike;
use foyer::{
    AdmitAllPicker, DirectFsDeviceOptions, Engine, FifoPicker, HybridCache, HybridCacheBuilder, HybridCachePolicy,
    IopsCounter, LargeEngineOptions, LruConfig, RecoverMode, RejectAllPicker, RuntimeOptions, SmallEngineOptions,
    Throttle, TokioRuntimeOptions, TombstoneLogConfigBuilder,
};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempdir()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .with_name("my-hybrid-cache")
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .memory(1024)
        .with_shards(4)
        .with_eviction_config(LruConfig {
            high_priority_pool_ratio: 0.1,
        })
        .with_hash_builder(BuildHasherDefault::default())
        .with_weighter(|_key, value: &String| value.len())
        .storage(Engine::Mixed {
            ratio: 0.1,
            large: LargeEngineOptions::new()
                .with_indexer_shards(64)
                .with_recover_concurrency(8)
                .with_flushers(2)
                .with_reclaimers(2)
                .with_buffer_pool_size(256 * 1024 * 1024)
                .with_clean_region_threshold(4)
                .with_eviction_pickers(vec![Box::<FifoPicker>::default()])
                .with_reinsertion_picker(Arc::<RejectAllPicker>::default())
                .with_tombstone_log_config(
                    TombstoneLogConfigBuilder::new(dir.path().join("tombstone-log-file"))
                        .with_flush(true)
                        .build(),
                ),
            small: SmallEngineOptions::new()
                .with_set_size(16 * 1024)
                .with_set_cache_capacity(64)
                .with_flushers(2),
        })
        .with_device_options(
            DirectFsDeviceOptions::new(dir.path())
                .with_capacity(64 * 1024 * 1024)
                .with_file_size(4 * 1024 * 1024)
                .with_throttle(
                    Throttle::new()
                        .with_read_iops(4000)
                        .with_write_iops(2000)
                        .with_write_throughput(100 * 1024 * 1024)
                        .with_read_throughput(800 * 1024 * 1024)
                        .with_iops_counter(IopsCounter::PerIoSize(NonZeroUsize::new(128 * 1024).unwrap())),
                ),
        )
        .with_recover_mode(RecoverMode::Quiet)
        .with_admission_picker(Arc::<AdmitAllPicker>::default())
        .with_compression(foyer::Compression::Lz4)
        .with_runtime_options(RuntimeOptions::Separated {
            read_runtime_options: TokioRuntimeOptions {
                worker_threads: 4,
                max_blocking_threads: 8,
            },
            write_runtime_options: TokioRuntimeOptions {
                worker_threads: 4,
                max_blocking_threads: 8,
            },
        })
        .build()
        .await?;

    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());
    assert_eq!(
        hybrid.get(&42).await?.unwrap().value(),
        "The answer to life, the universe, and everything."
    );

    let e = hybrid
        .fetch(20230512, || async {
            let value = mock().await?;
            Ok(value)
        })
        .await?;
    assert_eq!(e.key(), &20230512);
    assert_eq!(e.value(), "Hello, foyer.");

    hybrid.close().await.unwrap();

    Ok(())
}

async fn mock() -> Result<String> {
    let now = chrono::Utc::now();
    if format!("{}{}{}", now.year(), now.month(), now.day()) == "20230512" {
        return Err(anyhow::anyhow!("Hi, time traveler!"));
    }
    Ok("Hello, foyer.".to_string())
}
