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

use foyer::{HybridCache, HybridCacheBuilder, HybridCachePolicy, RecoverMode};
use foyer_opendal::OpenDalEngineConfig;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A fresh directory gives this cache exclusive ownership of its namespace.
    let directory = tempfile::tempdir()?;
    let operator = Operator::new(opendal_service_fs::Fs::default().root(directory.path().to_str().unwrap()))?;
    let engine = OpenDalEngineConfig::new(operator, "cache".into(), 64 << 20, 8 << 20);
    let cache: HybridCache<String, Vec<u8>> = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(8 << 20)
        .with_weighter(|key: &String, value: &Vec<u8>| key.len() + value.len())
        .storage()
        .with_recover_mode(RecoverMode::None)
        .with_engine_config(engine)
        .build()
        .await?;

    // Include source identity, immutable version, and byte range in every key.
    let key = "dataset-a/object-42/version-7/bytes-0-4096".to_string();
    let entry = cache
        .get_or_fetch(&key, || async {
            // A real source request must read version 7, not the latest object.
            Ok::<_, anyhow::Error>(vec![7; 4096])
        })
        .await?;
    println!("Fetched {} bytes", entry.value().len());
    drop(entry);

    cache.storage().wait().await;
    cache.memory().clear();
    let entry = cache.get(&key).await?.expect("entry in the OpenDAL cache");
    println!("Read {} bytes after clearing memory", entry.value().len());
    drop(entry);
    cache.close().await?;
    Ok(())
}
