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

use std::{
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::collections::VecDeque;
use parking_lot::Mutex;
use tokio::sync::Notify;


#[derive(Debug)]
pub struct AsyncQueue<T> {
    queue: Mutex<VecDeque<T>>,
    size: AtomicUsize,
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
            queue: Mutex::new(VecDeque::default()),
            size: AtomicUsize::new(0),
            notified: Notify::new(),
        }
    }

    pub async fn acquire(&self) -> T {
        loop {
            let notified = self.notified.notified();
            if let Some(item) = self.queue.lock().pop_front() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                break item;
            }
            notified.await;
        }
    }

    pub fn release(&self, item: T) {
        self.queue.lock().push_back(item);
        self.size.fetch_add(1, Ordering::Relaxed);
        // TODO: may optimize with `notify_one`
        self.notified.notify_waiters();
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
