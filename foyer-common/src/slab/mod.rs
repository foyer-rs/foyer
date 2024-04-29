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

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Token(NonZeroUsize);

impl Token {
    const MASK: usize = 1 << (usize::BITS - 1);

    fn new(index: usize) -> Self {
        unsafe { Self(NonZeroUsize::new_unchecked(index | Self::MASK)) }
    }

    pub fn index(&self) -> usize {
        self.0.get() & !Self::MASK
    }
}

pub struct Slab<T> {
    entries: Vec<Entry<T>>,
    len: usize,
    next: usize,
}

impl<T> Slab<T> {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            next: 0,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            next: 0,
            len: 0,
        }
    }

    pub fn insert(&mut self, val: T) -> Token {
        let index = self.next;
        self.insert_at(index, val);
        Token::new(index)
    }

    pub fn remove(&mut self, token: Token) -> Option<T> {
        self.remove_at(token.index())
    }

    /// Remove a value by token.
    ///
    /// # Safety
    ///
    /// The token must be valid.
    pub unsafe fn remove_unchecked(&mut self, token: Token) -> T {
        self.remove_at(token.index()).unwrap_unchecked()
    }

    pub fn get(&self, token: Token) -> Option<&T> {
        match self.entries.get(token.index()) {
            Some(Entry::Occupied(val)) => Some(val),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, token: Token) -> Option<&mut T> {
        match self.entries.get_mut(token.index()) {
            Some(Entry::Occupied(val)) => Some(val),
            _ => None,
        }
    }

    /// Remove the immutable reference of a value by token.
    ///
    /// # Safety
    ///
    /// The token must be valid.
    pub unsafe fn get_unchecked(&self, token: Token) -> &T {
        match self.entries.get_unchecked(token.index()) {
            Entry::Occupied(val) => val,
            _ => unreachable!(),
        }
    }

    /// Remove the mutable reference of a value by token.
    ///
    /// # Safety
    ///
    /// The token must be valid.
    pub unsafe fn get_unchecked_mut(&mut self, token: Token) -> &mut T {
        match self.entries.get_unchecked_mut(token.index()) {
            Entry::Occupied(val) => val,
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert_at(&mut self, index: usize, val: T) {
        self.len += 1;

        if index == self.entries.len() {
            self.entries.push(Entry::Occupied(val));
            self.next = index + 1;
        } else {
            self.next = match self.entries.get(index) {
                Some(&Entry::Vacant(next)) => next,
                _ => unreachable!(),
            };
            self.entries[index] = Entry::Occupied(val);
        }
    }

    fn remove_at(&mut self, index: usize) -> Option<T> {
        let entry = self.entries.get_mut(index)?;

        if matches!(entry, Entry::Vacant(_)) {
            return None;
        }

        let entry = std::mem::replace(entry, Entry::Vacant(self.next));

        match entry {
            Entry::Vacant(_) => unreachable!(),
            Entry::Occupied(val) => {
                self.len -= 1;
                self.next = index;
                Some(val)
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Entry<T> {
    Vacant(usize),
    Occupied(T),
}

#[cfg(test)]
mod tests;

pub mod slab_linked_list;
