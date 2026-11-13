// Copyright 2026 foyer Project Authors
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

use foyer::{
    DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, RecoverMode, SmallEngineConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let open = || async {
        let device = FsDeviceBuilder::new(dir.path()).with_capacity(1024 * 1024).build()?;
        HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(16)
            .with_shards(1)
            .storage()
            .with_engine_config(SmallEngineConfig::new(device).with_bucket_size(16 * 1024))
            .with_recover_mode(RecoverMode::Strict)
            .build()
            .await
    };
    let cache: HybridCache<String, Vec<u8>> = open().await?;
    cache.insert("small-file".to_owned(), vec![42; 1024]);
    cache.close().await?;
    drop(cache);

    // Reopen with an empty memory cache to verify the disk path.
    let cache: HybridCache<String, Vec<u8>> = open().await?;
    let entry = cache.get("small-file").await?.unwrap();
    assert_eq!(entry.value(), &vec![42; 1024]);
    drop(entry);
    cache.remove("small-file");
    cache.close().await?;
    drop(cache);

    let cache: HybridCache<String, Vec<u8>> = open().await?;
    assert!(cache.get("small-file").await?.is_none());
    cache.close().await?;
    Ok(())
}
