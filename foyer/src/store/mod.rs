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

pub mod error;
#[allow(dead_code)]
pub mod file;
pub mod read_only_file_store;

use std::sync::Arc;

use crate::{Data, Index, Metrics};
use async_trait::async_trait;

use error::Result;

#[async_trait]
pub trait Store: Send + Sync + Sized + 'static {
    type I: Index;
    type D: Data;
    type C: Send + Sync + Clone + std::fmt::Debug + 'static;

    async fn open(pool: usize, config: Self::C, metrics: Arc<Metrics>) -> Result<Self>;

    async fn store(&self, index: Self::I, data: Self::D) -> Result<()>;

    async fn load(&self, index: &Self::I) -> Result<Option<Self::D>>;

    async fn delete(&self, index: &Self::I) -> Result<()>;
}

async fn asyncify<F, T>(f: F) -> error::Result<T>
where
    F: FnOnce() -> error::Result<T> + Send + 'static,
    T: Send + 'static,
{
    #[cfg_attr(madsim, expect(deprecated))]
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(error::Error::Other("background task failed".to_string())),
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::RwLock;

    use super::error::Result;

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub struct MemoryStore<I: Index, D: Data + Clone> {
        inner: Arc<RwLock<HashMap<I, D>>>,
    }

    impl<I: Index, D: Data + Clone> MemoryStore<I, D> {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(RwLock::new(HashMap::default())),
            }
        }
    }

    #[async_trait]
    impl<I: Index, D: Data + Clone> Store for MemoryStore<I, D> {
        type I = I;

        type D = D;

        type C = ();

        async fn open(_pool: usize, _: Self::C, _metrics: Arc<Metrics>) -> Result<Self> {
            Ok(Self::new())
        }

        async fn store(&self, index: Self::I, data: Self::D) -> Result<()> {
            let mut inner = self.inner.write();
            inner.insert(index, data);
            Ok(())
        }

        async fn load(&self, index: &Self::I) -> Result<Option<Self::D>> {
            let inner = self.inner.read();
            Ok(inner.get(index).cloned())
        }

        async fn delete(&self, index: &Self::I) -> Result<()> {
            let mut inner = self.inner.write();
            inner.remove(index);
            Ok(())
        }
    }
}
