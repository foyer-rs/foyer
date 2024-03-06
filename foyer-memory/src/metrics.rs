//  Copyright 2024 MrCroxx
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

#[derive(Debug, Default)]
pub struct MetricsShard {
    /// successful inserts without replaces
    pub insert: usize,
    /// successful replaces
    pub replace: usize,

    /// get hits
    pub hit: usize,
    /// get misses
    pub miss: usize,

    /// fetches after cache miss with `entry` interface
    pub fetch: usize,
    /// deduped fetches after cache miss with `entry` interface
    pub queue: usize,

    /// successful removes
    pub remove: usize,

    /// evicts from the eviction container
    pub evict: usize,
    /// successful reinserts, only counts successful reinserts after evicted
    pub reinsert: usize,

    /// released handles
    pub release: usize,
}
