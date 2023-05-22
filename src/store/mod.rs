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

use crate::{Data, Index};

pub trait Store: Send + Sync + Clone + 'static {
    type I: Index;
    type D: Data;

    fn store(&self, index: Self::I, data: Self::D);

    fn load(&self, index: &Self::I) -> Option<Self::D>;

    fn delete(&self, index: &Self::I);
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::RwLock;

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

    impl<I: Index, D: Data + Clone> Store for MemoryStore<I, D> {
        type I = I;

        type D = D;

        fn store(&self, index: Self::I, data: Self::D) {
            let mut inner = self.inner.write();
            inner.insert(index, data);
        }

        fn load(&self, index: &Self::I) -> Option<Self::D> {
            let inner = self.inner.read();
            inner.get(index).cloned()
        }

        fn delete(&self, index: &Self::I) {
            let mut inner = self.inner.write();
            inner.remove(index);
        }
    }
}
