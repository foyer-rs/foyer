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

use std::fmt::Debug;

use foyer::{
    Deserialize, DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, HybridCachePolicy, Serialize,
    StorageValue,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Foo {
    a: u64,
    b: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Bar {
    a: u64,
    b: String,
}

impl Serialize for Bar {
    fn serialize(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.a.to_le_bytes())?;
        writer.write_all(&(self.b.len() as u64).to_le_bytes())?;
        writer.write_all(self.b.as_bytes())?;
        Ok(())
    }

    fn estimated_size(&self) -> usize {
        8 + 8 + self.b.len()
    }
}

impl Deserialize for Bar {
    fn deserialize(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let a = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf)?;
        let b_len = u64::from_le_bytes(buf) as usize;
        let bytes = vec![0u8; b_len];
        let b = String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(Self { a, b })
    }
}

async fn case<V: StorageValue + Clone + Eq + Debug>(value: V) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    let hybrid: HybridCache<u64, V> = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(64 * 1024 * 1024)
        .storage(Engine::Large) // use large object disk cache engine only
        .with_device_options(DirectFsDeviceOptions::new(dir.path()).with_capacity(256 * 1024 * 1024))
        .build()
        .await?;

    hybrid.insert(42, value.clone());
    assert_eq!(hybrid.get(&42).await?.unwrap().value(), &value);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    case("The answer to life, the universe, and everything.".to_string()).await?;

    case(Foo {
        a: 42,
        b: "The answer to life, the universe, and everything.".to_string(),
    })
    .await?;

    case(Bar {
        a: 42,
        b: "The answer to life, the universe, and everything.".to_string(),
    })
    .await?;

    Ok(())
}
