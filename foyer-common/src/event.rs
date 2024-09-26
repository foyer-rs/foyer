//  Copyright 2024 foyer Project Authors
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

use crate::code::{Key, Value};

/// Trait for the customized event listener.

pub trait EventListener: Send + Sync + 'static {
    /// Associated key type.
    type Key;
    /// Associated value type.
    type Value;

    /// Called when a cache entry is released from the in-memory cache.
    #[expect(unused_variables)]
    fn on_memory_release(&self, key: Self::Key, value: Self::Value)
    where
        Self::Key: Key,
        Self::Value: Value,
    {
    }
}
