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

use std::ptr::NonNull;

use crate::{handle::Handle, CacheContext};

use super::{fifo::Fifo, lfu::Lfu, lru::Lru, s3fifo::S3Fifo, Eviction};

pub struct EvictionQueue<T, E>
where
    E: Eviction,
    E::Handle: Handle<Data = T>,
{
    eviction: E,
}

impl<T, E> EvictionQueue<T, E>
where
    E: Eviction,
    E::Handle: Handle<Data = T>,
{
    pub fn new(capacity: usize, config: &E::Config) -> Self {
        let eviction = unsafe { E::new(capacity, config) };
        Self { eviction }
    }

    pub fn push(&mut self, data: <E::Handle as Handle>::Data) {
        self.push_with_context(data, CacheContext::default().into())
    }

    pub fn push_with_context(&mut self, data: <E::Handle as Handle>::Data, context: <E::Handle as Handle>::Context) {
        unsafe {
            let mut handle = Box::new(<E::Handle as Handle>::new());
            handle.init(0, data, 0, context);
            let ptr = NonNull::new_unchecked(Box::into_raw(handle));
            self.eviction.push(ptr)
        }
    }

    pub fn pop(&mut self) -> Option<<E::Handle as Handle>::Data> {
        self.pop_with_context().map(|(data, _)| data)
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    pub fn pop_with_context(&mut self) -> Option<(<E::Handle as Handle>::Data, <E::Handle as Handle>::Context)> {
        unsafe {
            let ptr = self.eviction.pop()?;
            let mut handle = Box::from_raw(ptr.as_ptr());
            let (data, context, _) = handle.base_mut().take();
            Some((data, context))
        }
    }

    pub fn len(&self) -> usize {
        self.eviction.len()
    }

    pub fn is_empty(&self) -> bool {
        self.eviction.is_empty()
    }
}

impl<T, E> Drop for EvictionQueue<T, E>
where
    E: Eviction,
    E::Handle: Handle<Data = T>,
{
    fn drop(&mut self) {
        unsafe {
            self.eviction.clear().into_iter().for_each(|ptr| {
                let _ = Box::from_raw(ptr.as_ptr());
            });
        }
    }
}

pub type FifoQueue<T> = EvictionQueue<T, Fifo<T>>;
pub type LruQueue<T> = EvictionQueue<T, Lru<T>>;
pub type LfuQueue<T> = EvictionQueue<T, Lfu<T>>;
pub type S3FifoQueue<T> = EvictionQueue<T, S3Fifo<T>>;

#[cfg(test)]
mod tests {
    use crate::FifoConfig;

    use super::*;

    #[test]
    fn test_eviction_queue_simple() {
        let mut fifo = FifoQueue::new(100, &FifoConfig {});
        (0..100).for_each(|i| fifo.push(i));
        (0..100).for_each(|i| assert_eq!(fifo.pop(), Some(i)));
    }

    #[test]
    fn test_eviction_queue_drop_clear() {
        let mut fifo = FifoQueue::new(100, &FifoConfig {});
        (0..100).for_each(|i| fifo.push(i));
    }
}
