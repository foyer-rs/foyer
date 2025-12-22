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
    BlockEngineConfig, Code, DeviceBuilder, Error, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy,
    Result, StorageValue,
};

#[cfg(feature = "serde")]
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

impl Code for Bar {
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<()> {
        writer.write_all(&self.a.to_le_bytes()).map_err(Error::io_error)?;
        writer
            .write_all(&(self.b.len() as u64).to_le_bytes())
            .map_err(Error::io_error)?;
        writer.write_all(self.b.as_bytes()).map_err(Error::io_error)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).map_err(Error::io_error)?;
        let a = u64::from_le_bytes(buf);
        reader.read_exact(&mut buf).map_err(Error::io_error)?;
        let b_len = u64::from_le_bytes(buf) as usize;
        let bytes = vec![0u8; b_len];
        let b = String::from_utf8(bytes)
            .map_err(std::io::Error::other)
            .map_err(Error::io_error)?;
        Ok(Self { a, b })
    }

    fn estimated_size(&self) -> usize {
        8 + 8 + self.b.len()
    }
}

async fn case<V: StorageValue + Clone + Eq + Debug>(value: V) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    let device = FsDeviceBuilder::new(dir.path())
        .with_capacity(256 * 1024 * 1024)
        .build()?;

    let hybrid: HybridCache<u64, V> = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(64 * 1024 * 1024)
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await?;

    hybrid.insert(42, value.clone());
    assert_eq!(hybrid.get(&42).await?.unwrap().value(), &value);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    case("The answer to life, the universe, and everything.".to_string()).await?;

    #[cfg(feature = "serde")]
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
