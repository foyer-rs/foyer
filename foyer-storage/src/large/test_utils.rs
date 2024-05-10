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

use std::{borrow::Borrow, collections::HashSet, fmt::Debug, hash::Hash, marker::PhantomData};

use super::admission::AdmissionPicker;

pub struct BiasedPicker<K, Q> {
    rejects: HashSet<Q>,
    _marker: PhantomData<K>,
}

impl<K, Q> Debug for BiasedPicker<K, Q>
where
    Q: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BiasedPicker").field("rejects", &self.rejects).finish()
    }
}

impl<K, Q> BiasedPicker<K, Q> {
    pub fn new(rejects: impl IntoIterator<Item = Q>) -> Self
    where
        Q: Hash + Eq,
    {
        Self {
            rejects: rejects.into_iter().collect(),
            _marker: PhantomData,
        }
    }
}

impl<K, Q> AdmissionPicker for BiasedPicker<K, Q>
where
    K: Send + Sync + 'static + Borrow<Q>,
    Q: Hash + Eq + Send + Sync + 'static + Debug,
{
    type Key = K;

    fn pick(&self, key: &Self::Key) -> bool {
        self.rejects.contains(key.borrow())
    }
}
