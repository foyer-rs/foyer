// Copyright 2026 foyer Project Authors
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

#[cfg(any(test, feature = "test_utils"))]
pub use crate::eviction::test_utils::TestProperties;
pub use crate::{
    cache::{Cache, CacheBuilder, CacheEntry, CacheProperties, EvictionConfig, GetOrFetch},
    eviction::{fifo::FifoConfig, lfu::LfuConfig, lru::LruConfig, s3fifo::S3FifoConfig, Eviction, Op},
    inflight::{
        FetchTarget, Notifier, OptionalFetch, OptionalFetchBuilder, RequiredFetch, RequiredFetchBuilder,
        RequiredFetchBuilderErased, Waiter,
    },
    pipe::{Piece, Pipe},
    raw::{Filter, Weighter},
};
