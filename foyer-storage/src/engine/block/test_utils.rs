// Copyright 2025 foyer Project Authors
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

use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::test_utils::EngineTestUtils;

/// [`FlushHolder`] is a simple holder to block flush operations when needed.
#[derive(Debug, Default)]
pub struct FlushHolder {
    hold: AtomicBool,
}

impl FlushHolder {
    /// Reurns whether the flush is currently held.
    pub fn is_held(&self) -> bool {
        self.hold.load(Ordering::Relaxed)
    }

    /// Holds the flush operation.
    pub fn hold(&self) {
        self.hold.store(true, Ordering::Relaxed);
    }

    /// Unholds the flush operation.
    pub fn unhold(&self) {
        self.hold.store(false, Ordering::Relaxed);
    }
}

impl EngineTestUtils for FlushHolder {}
