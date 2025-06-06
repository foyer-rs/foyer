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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Debug, Clone, Default)]
pub struct FlushHolder {
    hold: Arc<AtomicBool>,
}

impl FlushHolder {
    pub fn is_held(&self) -> bool {
        self.hold.load(Ordering::Relaxed)
    }

    pub fn hold(&self) {
        self.hold.store(true, Ordering::Relaxed);
    }

    pub fn unhold(&self) {
        self.hold.store(false, Ordering::Relaxed);
    }
}
