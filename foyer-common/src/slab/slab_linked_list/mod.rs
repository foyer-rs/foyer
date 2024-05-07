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

use super::{Slab, Token};

pub struct SlabLinkedList<T> {
    slab: Slab<SlabLinkedListNode<T>>,
    head: Option<Token>,
    tail: Option<Token>,
}

impl<T> Default for SlabLinkedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SlabLinkedList<T> {
    pub const fn new() -> Self {
        Self {
            slab: Slab::new(),
            head: None,
            tail: None,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slab: Slab::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    pub fn front(&self) -> Option<&T> {
        self.head
            .map(|token| unsafe { self.slab.get_unchecked(token).as_ref() })
    }

    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.head
            .map(|token| unsafe { self.slab.get_unchecked_mut(token).as_mut() })
    }

    pub fn back(&self) -> Option<&T> {
        self.tail
            .map(|token| unsafe { self.slab.get_unchecked(token).as_ref() })
    }

    pub fn back_mut(&mut self) -> Option<&mut T> {
        self.tail
            .map(|token| unsafe { self.slab.get_unchecked_mut(token).as_mut() })
    }

    pub fn push_front(&mut self, val: T) -> Token {
        self.iter_mut().insert_after(val)
    }

    pub fn push_back(&mut self, val: T) -> Token {
        self.iter_mut().insert_before(val)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        let mut iter = self.iter_mut();
        iter.move_forward();
        iter.remove()
    }

    pub fn pop_back(&mut self) -> Option<T> {
        let mut iter = self.iter_mut();
        iter.move_backward();
        iter.remove()
    }

    pub fn clear(&mut self) {
        let mut iter = self.iter_mut();
        iter.move_to_head();
        while iter.is_valid() {
            iter.remove();
        }
        assert!(self.is_empty());
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            token: None,
            list: self,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            token: None,
            list: self,
        }
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove a node by slab token.
    ///
    /// # Safety
    ///
    /// The slab token must be valid.
    pub unsafe fn remove_with_token(&mut self, token: Token) -> T {
        self.iter_mut_with_token(token).remove().unwrap_unchecked()
    }

    unsafe fn iter_mut_with_token(&mut self, token: Token) -> IterMut<'_, T> {
        IterMut {
            token: Some(token),
            list: self,
        }
    }
}

impl<T> Drop for SlabLinkedList<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T> IntoIterator for SlabLinkedList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { list: self }
    }
}

impl<T> Extend<T> for SlabLinkedList<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        iter.into_iter().for_each(|elt| {
            self.push_back(elt);
        })
    }
}

impl<T> FromIterator<T> for SlabLinkedList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut list = Self::new();
        list.extend(iter);
        list
    }
}

struct SlabLinkedListNode<T> {
    val: T,

    prev: Option<Token>,
    next: Option<Token>,
}

impl<T> AsRef<T> for SlabLinkedListNode<T> {
    fn as_ref(&self) -> &T {
        &self.val
    }
}

impl<T> AsMut<T> for SlabLinkedListNode<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.val
    }
}

pub struct IterMut<'a, T: 'a> {
    token: Option<Token>,
    list: &'a mut SlabLinkedList<T>,
}

impl<'a, T> IterMut<'a, T> {
    pub fn is_valid(&self) -> bool {
        self.token.is_some()
    }

    pub fn get(&self) -> Option<&T> {
        self.token
            .map(|token| unsafe { self.list.slab.get_unchecked(token).as_ref() })
    }

    pub fn get_mut(&mut self) -> Option<&mut T> {
        self.token
            .map(|token| unsafe { self.list.slab.get_unchecked_mut(token).as_mut() })
    }

    /// Move forward.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn move_forward(&mut self) {
        match self.token {
            Some(token) => unsafe { self.token = self.list.slab.get_unchecked(token).next },
            None => self.token = self.list.head,
        }
    }

    /// Move Backward.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn move_backward(&mut self) {
        match self.token {
            Some(token) => unsafe { self.token = self.list.slab.get_unchecked(token).prev },
            None => self.token = self.list.head,
        }
    }

    pub fn move_to_head(&mut self) {
        self.token = self.list.head
    }

    pub fn move_to_tail(&mut self) {
        self.token = self.list.tail
    }

    pub fn remove(&mut self) -> Option<T> {
        if !self.is_valid() {
            return None;
        }

        let token = unsafe { self.token.unwrap_unchecked() };
        let mut node = unsafe { self.list.slab.remove_unchecked(token) };

        if Some(token) == self.list.head {
            self.list.head = node.next;
        }
        if Some(token) == self.list.tail {
            self.list.tail = node.prev;
        }

        if let Some(token) = node.prev {
            unsafe { self.list.slab.get_unchecked_mut(token).next = node.next };
        }
        if let Some(token) = node.next {
            unsafe { self.list.slab.get_unchecked_mut(token).prev = node.prev };
        }

        self.token = node.next;

        node.next = None;
        node.prev = None;

        Some(node.val)
    }

    /// Link a new ptr before the current one.
    ///
    /// If iter is on null, link to tail.
    pub fn insert_before(&mut self, val: T) -> Token {
        let token_new = self.list.slab.insert(SlabLinkedListNode {
            val,
            prev: None,
            next: None,
        });

        match self.token {
            Some(token) => self.link_before(token_new, token),
            None => {
                self.link_between(token_new, self.list.tail, None);
                self.list.tail = Some(token_new)
            }
        }

        if self.list.head == self.token {
            self.list.head = Some(token_new);
        }

        token_new
    }

    /// Link a new ptr after the current one.
    ///
    /// If iter is on null, link to head.
    pub fn insert_after(&mut self, val: T) -> Token {
        let token_new = self.list.slab.insert(SlabLinkedListNode {
            val,
            prev: None,
            next: None,
        });

        match self.token {
            Some(token) => self.link_after(token_new, token),
            None => {
                self.link_between(token_new, None, self.list.head);
                self.list.head = Some(token_new)
            }
        }

        if self.list.tail == self.token {
            self.list.tail = Some(token_new);
        }

        token_new
    }

    pub fn is_head(&self) -> bool {
        self.token == self.list.head
    }

    pub fn is_tail(&self) -> bool {
        self.token == self.list.tail
    }

    fn link_before(&mut self, token: Token, next: Token) {
        self.link_between(token, unsafe { self.list.slab.get_unchecked(next).prev }, Some(next));
    }

    fn link_after(&mut self, token: Token, prev: Token) {
        self.link_between(token, Some(prev), unsafe { self.list.slab.get_unchecked(prev).next });
    }

    fn link_between(&mut self, token: Token, prev: Option<Token>, next: Option<Token>) {
        if let Some(prev) = prev {
            unsafe { self.list.slab.get_unchecked_mut(prev).next = Some(token) };
        }
        if let Some(next) = next {
            unsafe { self.list.slab.get_unchecked_mut(next).prev = Some(token) };
        }
        let node = unsafe { self.list.slab.get_unchecked_mut(token) };
        node.prev = prev;
        node.next = next;
    }
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.move_forward();
        self.get_mut().map(|val| unsafe { &mut *(val as *mut _) })
    }
}

pub struct Iter<'a, T: 'a> {
    token: Option<Token>,
    list: &'a SlabLinkedList<T>,
}

impl<'a, T> Iter<'a, T> {
    pub fn is_valid(&self) -> bool {
        self.token.is_some()
    }

    pub fn get(&self) -> Option<&T> {
        self.token
            .map(|token| unsafe { self.list.slab.get_unchecked(token).as_ref() })
    }

    /// Move forward.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn move_forward(&mut self) {
        match self.token {
            Some(token) => unsafe { self.token = self.list.slab.get_unchecked(token).next },
            None => self.token = self.list.head,
        }
    }

    /// Move Backward.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn move_backward(&mut self) {
        match self.token {
            Some(token) => unsafe { self.token = self.list.slab.get_unchecked(token).prev },
            None => self.token = self.list.head,
        }
    }

    pub fn move_to_head(&mut self) {
        self.token = self.list.head
    }

    pub fn move_to_tail(&mut self) {
        self.token = self.list.tail
    }

    pub fn is_head(&self) -> bool {
        self.token == self.list.head
    }

    pub fn is_tail(&self) -> bool {
        self.token == self.list.tail
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.move_forward();
        self.get().map(|val| unsafe { &*(val as *const _) })
    }
}

pub struct IntoIter<T> {
    list: SlabLinkedList<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.list.pop_front()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.list.len(), Some(self.list.len()))
    }
}

#[cfg(test)]
mod tests;
