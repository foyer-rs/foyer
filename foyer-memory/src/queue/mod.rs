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

pub mod generic;

use crate::{
    eviction::{fifo::Fifo, lfu::Lfu, lru::Lru, s3fifo::S3Fifo},
    CacheContext, FifoConfig, LfuConfig, LruConfig, S3FifoConfig,
};
use generic::GenericEvictionQueue;

pub type FifoQueue<T> = GenericEvictionQueue<T, Fifo<T>>;
pub type LruQueue<T> = GenericEvictionQueue<T, Lru<T>>;
pub type LfuQueue<T> = GenericEvictionQueue<T, Lfu<T>>;
pub type S3FifoQueue<T> = GenericEvictionQueue<T, S3Fifo<T>>;

pub enum EvictionQueueConfig {
    Fifo(FifoConfig),
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

pub enum EvictionQueue<T>
where
    T: Send + Sync + 'static,
{
    Fifo(FifoQueue<T>),
    Lru(LruQueue<T>),
    Lfu(LfuQueue<T>),
    S3Fifo(S3FifoQueue<T>),
}

impl<T> EvictionQueue<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(capacity: usize, config: &EvictionQueueConfig) -> Self {
        match config {
            EvictionQueueConfig::Fifo(c) => Self::Fifo(FifoQueue::new(capacity, c)),
            EvictionQueueConfig::Lru(c) => Self::Lru(LruQueue::new(capacity, c)),
            EvictionQueueConfig::Lfu(c) => Self::Lfu(LfuQueue::new(capacity, c)),
            EvictionQueueConfig::S3Fifo(c) => Self::S3Fifo(S3FifoQueue::new(capacity, c)),
        }
    }

    pub fn push(&mut self, data: T) {
        match self {
            EvictionQueue::Fifo(queue) => queue.push(data),
            EvictionQueue::Lru(queue) => queue.push(data),
            EvictionQueue::Lfu(queue) => queue.push(data),
            EvictionQueue::S3Fifo(queue) => queue.push(data),
        }
    }

    pub fn push_with_context(&mut self, data: T, context: CacheContext) {
        match self {
            EvictionQueue::Fifo(queue) => queue.push_with_context(data, context.into()),
            EvictionQueue::Lru(queue) => queue.push_with_context(data, context.into()),
            EvictionQueue::Lfu(queue) => queue.push_with_context(data, context.into()),
            EvictionQueue::S3Fifo(queue) => queue.push_with_context(data, context.into()),
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        match self {
            EvictionQueue::Fifo(queue) => queue.pop(),
            EvictionQueue::Lru(queue) => queue.pop(),
            EvictionQueue::Lfu(queue) => queue.pop(),
            EvictionQueue::S3Fifo(queue) => queue.pop(),
        }
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    pub fn pop_with_context(&mut self) -> Option<(T, CacheContext)> {
        match self {
            EvictionQueue::Fifo(queue) => queue.pop_with_context().map(|(data, context)| (data, context.into())),
            EvictionQueue::Lru(queue) => queue.pop_with_context().map(|(data, context)| (data, context.into())),
            EvictionQueue::Lfu(queue) => queue.pop_with_context().map(|(data, context)| (data, context.into())),
            EvictionQueue::S3Fifo(queue) => queue.pop_with_context().map(|(data, context)| (data, context.into())),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EvictionQueue::Fifo(queue) => queue.len(),
            EvictionQueue::Lru(queue) => queue.len(),
            EvictionQueue::Lfu(queue) => queue.len(),
            EvictionQueue::S3Fifo(queue) => queue.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            EvictionQueue::Fifo(queue) => queue.is_empty(),
            EvictionQueue::Lru(queue) => queue.is_empty(),
            EvictionQueue::Lfu(queue) => queue.is_empty(),
            EvictionQueue::S3Fifo(queue) => queue.is_empty(),
        }
    }
}
