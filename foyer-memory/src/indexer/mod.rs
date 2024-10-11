//  Copyright 2024 foyer Project Authors
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

use std::{hash::Hash, ptr::NonNull};

use equivalent::Equivalent;
use foyer_common::code::Key;

use crate::handle::KeyedHandle;

pub trait Indexer: Send + Sync + 'static {
    type Key: Key;
    type Handle: KeyedHandle<Key = Self::Key>;

    fn new() -> Self;
    unsafe fn insert(&mut self, ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>>;
    unsafe fn get<Q>(&self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Q: Hash + Equivalent<Self::Key> + ?Sized;
    unsafe fn remove<Q>(&mut self, hash: u64, key: &Q) -> Option<NonNull<Self::Handle>>
    where
        Q: Hash + Equivalent<Self::Key> + ?Sized;
    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>>;
}

pub mod hash_table;
pub mod sanity;
