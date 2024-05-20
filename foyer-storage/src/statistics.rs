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

use std::sync::atomic::{AtomicUsize, Ordering};

/// The statistics of the disk cache, which is used by the pickers.
#[derive(Debug, Default)]
pub struct Statistics {
    pub(crate) cache_write_bytes: AtomicUsize,
    pub(crate) cache_read_bytes: AtomicUsize,
}

impl Statistics {
    /// Get the disk cache written bytes.
    pub fn cache_write_bytes(&self) -> usize {
        self.cache_write_bytes.load(Ordering::Relaxed)
    }

    /// Get the disk cache read bytes.
    pub fn cache_read_bytes(&self) -> usize {
        self.cache_read_bytes.load(Ordering::Relaxed)
    }
}
