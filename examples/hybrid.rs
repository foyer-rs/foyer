//  Copyright 2024 Foyer Project Authors
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

use std::sync::Arc;

use chrono::Datelike;
use foyer::{
    CacheContext, FsDeviceConfigBuilder, HybridCache, HybridCacheBuilder, LfuConfig, LruConfig,
    RatedTicketAdmissionPolicy, RatedTicketReinsertionPolicy, RuntimeConfigBuilder,
};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;

    let hybrid: HybridCache<u64, Arc<String>> = HybridCacheBuilder::new()
        .memory(1024)
        .with_shards(4)
        .with_eviction_config(LruConfig {
            high_priority_pool_ratio: 0.1,
        })
        .with_object_pool_capacity(1024)
        .with_hash_builder(ahash::RandomState::default())
        .with_weighter(|_key, value: &Arc<String>| value.len())
        .storage()
        .with_name("foyer")
        .with_eviction_config(LfuConfig {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        })
        .with_device_config(
            FsDeviceConfigBuilder::new(dir.path())
                .with_capacity(64 * 1024 * 1024)
                .with_file_size(4 * 1024 * 1024)
                .with_align(4 * 1024)
                .with_io_size(16 * 1024)
                .build(),
        )
        .with_admission_policy(Arc::new(RatedTicketAdmissionPolicy::new(10 * 1024 * 1024)))
        .with_reinsertion_policy(Arc::new(RatedTicketReinsertionPolicy::new(10 * 1024 * 1024)))
        .with_flushers(2)
        .with_reclaimers(2)
        .with_clean_region_threshold(2)
        .with_recover_concurrency(4)
        .with_compression(foyer::Compression::Lz4)
        .with_runtime_config(
            RuntimeConfigBuilder::new()
                .with_thread_name("foyer")
                .with_worker_threads(4)
                .build(),
        )
        .with_lazy(true)
        .build()
        .await?;

    hybrid.insert(
        42,
        Arc::new("The answer to life, the universe, and everything.".to_string()),
    );

    assert_eq!(
        hybrid.get(&42).await?.unwrap().value().as_ref(),
        "The answer to life, the universe, and everything."
    );

    hybrid.entry(20230512, || async {
        // fetch a value from a remote service
        let future = async {
            let now = chrono::Utc::now();
            if format!("{}{}{}", now.year(), now.month(), now.day()) == "20230512" {
                return Err(anyhow::anyhow!("Hi, time traveler!"));
            }
            Ok("Hello, foyer.".to_string())
        };

        let value = Arc::new(future.await?);
        Ok((value, CacheContext::default()))
    });

    Ok(())
}
