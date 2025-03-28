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

//! Utilities for testing.

use std::sync::Arc;

use foyer_common::code::{Key, Value};
use parking_lot::{Mutex, MutexGuard};

use crate::{Piece, Pipe};

/// A pipe that records all sent pieces.
#[derive(Debug)]
pub struct PiecePipe<K, V> {
    pieces: Arc<Mutex<Vec<Piece<K, V>>>>,
}

impl<K, V> Clone for PiecePipe<K, V> {
    fn clone(&self) -> Self {
        Self {
            pieces: self.pieces.clone(),
        }
    }
}

impl<K, V> Default for PiecePipe<K, V> {
    fn default() -> Self {
        Self {
            pieces: Default::default(),
        }
    }
}

impl<K, V> Pipe for PiecePipe<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn is_enabled(&self) -> bool {
        true
    }

    fn send(&self, piece: Piece<Self::Key, Self::Value>) {
        self.pieces.lock().push(piece);
    }
}

impl<K, V> PiecePipe<K, V> {
    /// Get all sent pieces.
    pub fn pieces(&self) -> MutexGuard<'_, Vec<Piece<K, V>>> {
        self.pieces.lock()
    }
}
