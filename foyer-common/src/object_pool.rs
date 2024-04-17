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

use crossbeam::queue::ArrayQueue;

pub struct ObjectPool<T> {
    inner: Arc<ObjectPoolInner<T>>,
}

impl<T> Clone for ObjectPool<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct ObjectPoolInner<T> {
    queue: Option<ArrayQueue<T>>,
    create: Box<dyn Fn() -> T + Send + Sync + 'static>,
}

impl<T> ObjectPool<T>
where
    T: Default + 'static,
{
    pub fn new(capacity: usize) -> Self {
        Self::new_with_create(capacity, T::default)
    }
}

impl<T> ObjectPool<T> {
    pub fn new_with_create(capacity: usize, create: impl Fn() -> T + Send + Sync + 'static) -> Self {
        let inner = ObjectPoolInner {
            queue: if capacity == 0 {
                None
            } else {
                Some(ArrayQueue::new(capacity))
            },
            create: Box::new(create),
        };
        Self { inner: Arc::new(inner) }
    }

    pub fn acquire(&self) -> T {
        match self.inner.queue.as_ref() {
            Some(queue) => queue.pop().unwrap_or((self.inner.create)()),
            None => (self.inner.create)(),
        }
    }

    pub fn release(&self, item: T) {
        if let Some(queue) = self.inner.queue.as_ref() {
            let _ = queue.push(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(capacity: usize) {
        let pool: ObjectPool<usize> = ObjectPool::new(capacity);

        for i in 0..capacity * 2 {
            pool.release(i);
        }

        for i in 0..capacity {
            assert_eq!(pool.acquire(), i);
        }

        for _ in 0..capacity {
            assert_eq!(pool.acquire(), 0);
        }
    }

    #[test]
    fn test_object_pool() {
        case(100);
    }

    #[test]
    fn test_object_pool_zero() {
        case(0);
    }
}
