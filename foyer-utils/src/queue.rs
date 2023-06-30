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

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};

#[derive(Debug)]
pub struct AsyncQueue<T: Debug> {
    tx: UnboundedSender<T>,
    rx: Mutex<UnboundedReceiver<T>>,

    size: AtomicUsize,
}

impl<T: Debug> Default for AsyncQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Debug> AsyncQueue<T> {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
            size: AtomicUsize::new(0),
        }
    }

    pub async fn acquire(&self) -> T {
        let mut rx = self.rx.lock().await;
        let item = rx.recv().await.unwrap();
        self.size.fetch_sub(1, Ordering::Relaxed);
        item
    }

    pub fn release(&self, item: T) {
        self.size.fetch_add(1, Ordering::Relaxed);
        self.tx.send(item).unwrap();
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
