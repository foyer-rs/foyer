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

use std::hash::RandomState;

use foyer::memory::{Cache, DefaultCacheEventListener, LruCacheConfig, LruConfig};

fn main() {
    let cache = Cache::new(
        LruCacheConfig {
            capacity: 16,
            shards: 4,
            eviction_config: LruConfig {
                high_priority_pool_ratio: 0.1,
            },
            object_pool_capacity: 1024,
            hash_builder: RandomState::default(),
            event_listener: DefaultCacheEventListener::default(),
        }
        .into(),
    );

    let entry = cache.insert("hello".to_string(), "world".to_string(), 1);
    let e = cache.get("hello").unwrap();

    assert_eq!(entry.value(), e.value());
}
