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

use foyer::{Cache, CacheBuilder, EventListener, FifoConfig};

struct EchoEventListener;

impl EventListener for EchoEventListener {
    type Key = u64;
    type Value = String;

    fn on_memory_release(&self, key: Self::Key, value: Self::Value)
    where
        Self::Key: foyer::Key,
        Self::Value: foyer::Value,
    {
        println!("Entry [key = {key}] [value = {value}] is released.")
    }
}

/// Output:
///
/// ```plain
/// Entry [key = 2] [value = First] is released.
/// Entry [key = 1] [value = Second] is released.
/// Entry [key = 3] [value = Third] is released.
/// Entry [key = 3] [value = Forth] is released.
/// ```
fn main() {
    let cache: Cache<u64, String> = CacheBuilder::new(2)
        .with_event_listener(Arc::new(EchoEventListener))
        .with_eviction_config(FifoConfig::default())
        .with_shards(1)
        .build();

    cache.insert(1, "Second".to_string());
    cache.deposit(2, "First".to_string());
    cache.insert(3, "Third".to_string());
    cache.insert(3, "Forth".to_string());
}
