// Copyright 2026 foyer Project Authors
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

use std::{fmt::Debug, future::Future, pin::Pin, sync::Arc};

use foyer_common::{
    code::{Key, Value},
    properties::Properties,
};
use parking_lot::{Mutex, MutexGuard};

use crate::{Piece, Pipe};

/// A pipe that records all sent pieces.
pub struct PiecePipe<K, V, P> {
    pieces: Arc<Mutex<Vec<Piece<K, V, P>>>>,
}

impl<K, V, P> Debug for PiecePipe<K, V, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PiecePipe").field("pieces", &self.pieces).finish()
    }
}

impl<K, V, P> Clone for PiecePipe<K, V, P> {
    fn clone(&self) -> Self {
        Self {
            pieces: self.pieces.clone(),
        }
    }
}

impl<K, V, P> Default for PiecePipe<K, V, P> {
    fn default() -> Self {
        Self {
            pieces: Default::default(),
        }
    }
}

impl<K, V, P> Pipe for PiecePipe<K, V, P>
where
    K: Key,
    V: Value,
    P: Properties,
{
    type Key = K;
    type Value = V;
    type Properties = P;

    fn is_enabled(&self) -> bool {
        true
    }

    fn send(&self, piece: Piece<Self::Key, Self::Value, Self::Properties>) {
        self.pieces.lock().push(piece);
    }

    fn flush(
        &self,
        mut pieces: Vec<Piece<Self::Key, Self::Value, Self::Properties>>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.pieces.lock().append(&mut pieces);
        Box::pin(async {})
    }
}

impl<K, V, P> PiecePipe<K, V, P> {
    /// Get all sent pieces.
    pub fn pieces(&self) -> MutexGuard<'_, Vec<Piece<K, V, P>>> {
        self.pieces.lock()
    }
}
