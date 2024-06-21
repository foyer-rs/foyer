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

use std::sync::atomic::AtomicUsize;

/// Configurations for trace.
#[derive(Debug)]
pub struct TraceConfig {
    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation in ns.
    pub record_hybrid_insert_threshold_ns: AtomicUsize,
    /// Threshold for recording the hybrid cache `get` operation in ns.
    pub record_hybrid_get_threshold_ns: AtomicUsize,
    /// Threshold for recording the hybrid cache `obtain` operation in ns.
    pub record_hybrid_obtain_threshold_ns: AtomicUsize,
    /// Threshold for recording the hybrid cache `remove` operation in ns.
    pub record_hybrid_remove_threshold_ns: AtomicUsize,
    /// Threshold for recording the hybrid cache `fetch` operation in ns.
    pub record_hybrid_fetch_threshold_ns: AtomicUsize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            record_hybrid_insert_threshold_ns: AtomicUsize::from(1000 * 1000 * 1000),
            record_hybrid_get_threshold_ns: AtomicUsize::from(1000 * 1000 * 1000),
            record_hybrid_obtain_threshold_ns: AtomicUsize::from(1000 * 1000 * 1000),
            record_hybrid_remove_threshold_ns: AtomicUsize::from(1000 * 1000 * 1000),
            record_hybrid_fetch_threshold_ns: AtomicUsize::from(1000 * 1000 * 1000),
        }
    }
}
