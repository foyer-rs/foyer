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

use std::{
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use foyer_common::code::StorageKey;
use foyer_memory::Piece;
use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};
use parking_lot::RwLock;

struct Pending<K, V, P> {
    piece: Piece<K, V, P>,
    current: Arc<AtomicBool>,
}

type Shard<K, V, P> = HashTable<Pending<K, V, P>>;

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
        let current = Arc::new(AtomicBool::new(true));
        match guard.entry(piece.hash(), |p| piece.key() == p.piece.key(), |p| p.piece.hash()) {
            HashTableEntry::Occupied(mut o) => {
                o.get().current.store(false, Ordering::Relaxed);
                *o.get_mut() = Pending {
                    piece: piece.clone(),
                    current: current.clone(),
                };
            }
            HashTableEntry::Vacant(v) => {
                v.insert(Pending {
                    piece: piece.clone(),
                    current: current.clone(),
                });
            }
        }
        drop(guard);
        PieceRef {
            piece,
            shard: Some(shard),
            current,
        }
    }

    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<Piece<K, V, P>>
    where
        Q: Hash + equivalent::Equivalent<K> + ?Sized,
    {
        let shard = self.shard(hash);
        let shard = shard.read();
        shard
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
        shard.find(hash, |p| key.equivalent(p.piece.key())).is_some()
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
    current: Arc<AtomicBool>,
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
    /// The result is a snapshot and may become stale immediately after the call.
    pub fn is_current(&self) -> bool {
        // The flag tracks registration identity, not publication of the entry data.
        self.current.load(Ordering::Relaxed)
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
            current: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<K, V, P> Drop for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn drop(&mut self) {
        // Superseded registrations never become current again.
        if !self.is_current() {
            return;
        }
        if let Some(shard) = self.shard.take() {
            let mut shard = shard.write();
            match shard.entry(self.hash(), |p| self.key() == p.piece.key(), |p| p.piece.hash()) {
                HashTableEntry::Occupied(o) => {
                    // A replacement may have occurred before acquiring the write lock.
                    if Arc::ptr_eq(&o.get().current, &self.current) {
                        self.current.store(false, Ordering::Relaxed);
                        o.remove();
                    }
                }
                HashTableEntry::Vacant(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder};

    use super::*;

    #[test]
    fn test_keeper_registration_lifecycle() {
        let memory: Cache<u64, u64> = CacheBuilder::new(16).with_shards(1).build();
        let keeper = Keeper::new(1);

        // Replacing a value: dropping the old reference must preserve the new value.
        let old = keeper.insert(memory.insert(1, 10).piece());
        let piece = memory.insert(1, 20).piece();
        let current = keeper.insert(piece.clone());

        assert!(!old.is_current());
        drop(old);
        assert!(current.is_current());
        assert_eq!(*keeper.get(piece.hash(), &1).unwrap().value(), 20);

        // Repeated submissions of the same piece must have independent registrations.
        let duplicate = keeper.insert(piece.clone());
        assert!(!current.is_current());
        drop(current);
        assert!(duplicate.is_current());

        // Deleting and reinserting: a stale reference must neither become current again nor remove the new entry.
        let latest = keeper.insert(piece.clone());
        drop(latest);
        assert!(!duplicate.is_current());
        assert!(keeper.get(piece.hash(), &1).is_none());

        let reinserted = keeper.insert(piece);
        assert!(!duplicate.is_current());
        drop(duplicate);
        assert!(reinserted.is_current());

        // A reference created without registering must not affect the current registration.
        let unregistered = PieceRef::from((*reinserted).clone());
        assert!(!unregistered.is_current());
        drop(unregistered);
        assert!(reinserted.is_current());

        // Checking the current registration must not wait for a shard writer.
        let shard = keeper.shard(reinserted.hash());
        let guard = shard.write();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| tx.send(reinserted.is_current()).unwrap());
            let result = rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(guard);
            assert_eq!(result, Ok(true));
        });
    }
}
