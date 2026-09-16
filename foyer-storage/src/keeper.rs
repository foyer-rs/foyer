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

use std::{fmt::Debug, hash::Hash, ops::Deref, sync::Arc};

use foyer_common::code::StorageKey;
use foyer_memory::Piece;
use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};
use parking_lot::RwLock;

struct Pending<K, V, P> {
    piece: Piece<K, V, P>,
    id: u64,
}

struct Shard<K, V, P> {
    pieces: HashTable<Pending<K, V, P>>,
    next_id: u64,
}

impl<K, V, P> Default for Shard<K, V, P> {
    fn default() -> Self {
        Self {
            pieces: HashTable::new(),
            next_id: 0,
        }
    }
}

struct Inner<K, V, P>
where
    K: StorageKey,
{
    shards: Vec<Arc<RwLock<Shard<K, V, P>>>>,
}

pub struct Keeper<K, V, P>
where
    K: StorageKey,
{
    inner: Arc<Inner<K, V, P>>,
}

impl<K, V, P> Debug for Keeper<K, V, P>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keeper")
            .field("shards", &self.inner.shards.len())
            .finish()
    }
}

impl<K, V, P> Keeper<K, V, P>
where
    K: StorageKey,
{
    pub fn new(shards: usize) -> Self {
        let shards = (0..shards).map(|_| Arc::new(RwLock::new(Shard::default()))).collect();
        Self {
            inner: Arc::new(Inner { shards }),
        }
    }

    pub fn insert(&self, piece: Piece<K, V, P>) -> PieceRef<K, V, P> {
        let shard = self.shard(piece.hash());

        let mut guard = shard.write();
        let id = guard.next_id;
        guard.next_id = guard.next_id.wrapping_add(1);
        match guard
            .pieces
            .entry(piece.hash(), |p| piece.key() == p.piece.key(), |p| p.piece.hash())
        {
            HashTableEntry::Occupied(mut o) => {
                *o.get_mut() = Pending {
                    piece: piece.clone(),
                    id,
                };
            }
            HashTableEntry::Vacant(v) => {
                v.insert(Pending {
                    piece: piece.clone(),
                    id,
                });
            }
        }
        drop(guard);
        PieceRef {
            piece,
            shard: Some(shard),
            id,
        }
    }

    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<Piece<K, V, P>>
    where
        Q: Hash + equivalent::Equivalent<K> + ?Sized,
    {
        let shard = self.shard(hash);
        let shard = shard.read();
        shard
            .pieces
            .find(hash, |p| key.equivalent(p.piece.key()))
            .map(|p| p.piece.clone())
    }

    /// Check if the keeper holds a piece with the given key without cloning it.
    pub fn contains<Q>(&self, hash: u64, key: &Q) -> bool
    where
        Q: Hash + equivalent::Equivalent<K> + ?Sized,
    {
        let shard = self.shard(hash);
        let shard = shard.read();
        shard.pieces.find(hash, |p| key.equivalent(p.piece.key())).is_some()
    }

    fn shard(&self, hash: u64) -> Arc<RwLock<Shard<K, V, P>>> {
        let index = (hash as usize) % self.inner.shards.len();
        self.inner.shards[index].clone()
    }
}

/// A reference to an in-memory cache [`Piece`] that is held while the piece sits in the disk
/// cache write queue.
///
/// Handed to [`crate::Engine::enqueue`] implementations, and dereferences to the [`Piece`]
/// it wraps.
pub struct PieceRef<K, V, P>
where
    K: StorageKey,
{
    piece: Piece<K, V, P>,
    // TODO(MrCroxx): Remove `Option`?
    shard: Option<Arc<RwLock<Shard<K, V, P>>>>,
    id: u64,
}

impl<K, V, P> Debug for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceRef").field("piece", &self.piece).finish()
    }
}

impl<K, V, P> Deref for PieceRef<K, V, P>
where
    K: StorageKey,
{
    type Target = Piece<K, V, P>;

    fn deref(&self) -> &Self::Target {
        &self.piece
    }
}

impl<K, V, P> PieceRef<K, V, P>
where
    K: StorageKey,
{
    /// Return whether this reference owns the current keeper registration.
    ///
    /// Engines that coalesce submissions can retain the current registration
    /// while dropping superseded ones, even if concurrent submissions reach
    /// the engine in a different order from their keeper registration.
    /// A reference constructed directly from a piece has no registration.
    pub fn is_current(&self) -> bool {
        self.shard.as_ref().is_some_and(|shard| {
            shard
                .read()
                .pieces
                .find(self.hash(), |p| self.key() == p.piece.key())
                .is_some_and(|p| p.id == self.id)
        })
    }
}

impl<K, V, P> From<Piece<K, V, P>> for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn from(piece: Piece<K, V, P>) -> Self {
        PieceRef {
            piece,
            shard: None,
            id: 0,
        }
    }
}

impl<K, V, P> Drop for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn drop(&mut self) {
        if let Some(shard) = self.shard.take() {
            let mut shard = shard.write();
            match shard
                .pieces
                .entry(self.hash(), |p| self.key() == p.piece.key(), |p| p.piece.hash())
            {
                HashTableEntry::Occupied(o) => {
                    if o.get().id == self.id {
                        o.remove();
                    }
                }
                HashTableEntry::Vacant(_) => {}
            }
        }
    }
}
