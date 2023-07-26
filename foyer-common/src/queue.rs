//  Copyright 2023 MrCroxx
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

use parking_lot::RwLock;
use std::{collections::VecDeque, fmt::Debug};
use tokio::sync::Notify;

#[derive(Debug)]
pub struct AsyncQueue<T> {
    queue: RwLock<VecDeque<T>>,
    notified: Notify,
}

impl<T: Debug> Default for AsyncQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Debug> AsyncQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: RwLock::new(VecDeque::default()),
            notified: Notify::new(),
        }
    }

    pub async fn acquire(&self) -> T {
        loop {
            let notified = self.notified.notified();
            {
                let mut guard = self.queue.write();
                if let Some(item) = guard.pop_front() {
                    if !guard.is_empty() {
                        // Since in `release` we use `notify_one`, not all waiters
                        // will be waken up. Therefore if we figure out that the queue is not empty,
                        // we call `notify_one` to awake the next pending `acquire`.
                        self.notified.notify_one();
                    }
                    break item;
                }
            }
            notified.await;
        }
    }

    pub fn release(&self, item: T) {
        self.queue.write().push_back(item);
        self.notified.notify_one();
    }

    pub fn len(&self) -> usize {
        self.queue.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::AsyncQueue;
    use std::{
        future::{poll_fn, Future},
        pin::pin,
        task::{Poll, Poll::Pending},
    };

    #[tokio::test]
    async fn test_basic() {
        let queue = AsyncQueue::new();
        queue.release(1);
        assert_eq!(1, queue.acquire().await);
    }

    #[tokio::test]
    async fn test_multiple_reader() {
        let queue = AsyncQueue::new();
        let mut read_future1 = pin!(queue.acquire());
        let mut read_future2 = pin!(queue.acquire());
        assert_eq!(
            Pending,
            poll_fn(|cx| Poll::Ready(read_future1.as_mut().poll(cx))).await
        );
        assert_eq!(
            Pending,
            poll_fn(|cx| Poll::Ready(read_future2.as_mut().poll(cx))).await
        );
        queue.release(1);
        queue.release(2);
        assert_eq!(1, read_future1.await);
        assert_eq!(2, read_future2.await);
    }
}
