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

use std::{fmt::Debug, marker::PhantomData};

pub trait ReinsertionPicker: Send + Sync + 'static + Debug {
    type Key;

    fn pick(&self, key: &Self::Key) -> bool;
}

pub struct RejectAllPicker<K>(PhantomData<K>)
where
    K: Send + Sync + 'static;

impl<K> Debug for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RejectAllPicker").finish()
    }
}

impl<K> Default for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K> ReinsertionPicker for RejectAllPicker<K>
where
    K: Send + Sync + 'static,
{
    type Key = K;

    fn pick(&self, _: &Self::Key) -> bool {
        false
    }
}
