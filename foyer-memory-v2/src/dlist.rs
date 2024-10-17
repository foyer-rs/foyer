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

use std::num::NonZeroUsize;

use foyer_common::{assert::OptionExt, strict_assert};
use slab::Slab;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlabToken(NonZeroUsize);

impl SlabToken {
    const MASK: usize = 1 << (usize::BITS - 1);

    pub fn from_raw(raw: usize) -> Self {
        // Assert the highest bit is not used.
        assert_eq!(0, raw & Self::MASK);
        let inner = unsafe { NonZeroUsize::new_unchecked(raw | Self::MASK) };
        Self(inner)
    }

    pub fn to_raw(&self) -> usize {
        self.0.get() & !Self::MASK
    }
}

#[repr(C)]
struct Node<T> {
    prev: Option<SlabToken>,
    next: Option<SlabToken>,

    data: T,
}

impl<T> Node<T> {
    fn new(data: T) -> Self {
        Self {
            prev: None,
            next: None,
            data,
        }
    }
}

pub struct SlabLinkedList<T> {
    head: Option<SlabToken>,
    tail: Option<SlabToken>,
    slab: Slab<Node<T>>,
    len: usize,
}

impl<T> Default for SlabLinkedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SlabLinkedList<T> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            head: None,
            tail: None,
            slab: Slab::with_capacity(capacity),
            len: 0,
        }
    }

    /// Get the reference of the first item of the double linked list.
    pub fn front(&self) -> Option<&T> {
        self.head.map(|token| &self.get_node(token).data)
    }

    /// Get the reference of the last item of the double linked list.
    pub fn back(&self) -> Option<&T> {
        self.tail.map(|token| &self.get_node(token).data)
    }

    /// Get the mutable reference of the first item of the double linked list.
    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.head.map(|token| &mut self.get_node_mut(token).data)
    }

    /// Get the mutable reference of the last item of the double linked list.
    pub fn back_mut(&mut self) -> Option<&mut T> {
        self.tail.map(|token| &mut self.get_node_mut(token).data)
    }

    /// Push an item to the first position of the double linked list.
    pub fn push_front(&mut self, data: T) {
        self.iter_mut().insert_after(data);
    }

    /// Push an item to the last position of the double linked list.
    pub fn push_back(&mut self, data: T) {
        self.iter_mut().insert_before(data);
    }

    /// Pop an item from the first position of the double linked list.
    pub fn pop_front(&mut self) -> Option<T> {
        let mut iter = self.iter_mut();
        iter.next();
        iter.remove()
    }

    /// Pop an item from the last position of the double linked list.
    pub fn pop_back(&mut self) -> Option<T> {
        let mut iter = self.iter_mut();
        iter.prev();
        iter.remove()
    }

    /// Get the item reference iterator of the double linked list.
    pub fn iter(&self) -> SlabLinkedListIter<'_, T> {
        SlabLinkedListIter {
            token: None,
            list: self,
        }
    }

    /// Get the item mutable reference iterator of the double linked list.
    pub fn iter_mut(&mut self) -> SlabLinkedListIterMut<'_, T> {
        SlabLinkedListIterMut {
            token: None,
            list: self,
        }
    }

    /// Get the length of the double linked list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the double linked list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove an node with slab token.
    ///
    /// # Safety
    ///
    /// `token` MUST be in this double linked list.
    pub unsafe fn remove_raw(&mut self, token: SlabToken) -> T {
        let mut iter = self.iter_mut_from_raw(token);
        strict_assert!(iter.is_valid());
        iter.remove().strict_unwrap_unchecked()
    }

    /// Create mutable iterator directly on slab token.
    ///
    /// # Safety
    ///
    /// `token` MUST be in this double linked list.
    pub unsafe fn iter_mut_from_raw(&mut self, token: SlabToken) -> SlabLinkedListIterMut<'_, T> {
        SlabLinkedListIterMut {
            token: Some(token),
            list: self,
        }
    }

    /// Create immutable iterator directly on slab token.
    ///
    /// # Safety
    ///
    /// `token` MUST be in this double linked list.
    pub unsafe fn iter_from_raw(&self, token: SlabToken) -> SlabLinkedListIter<'_, T> {
        SlabLinkedListIter {
            token: Some(token),
            list: self,
        }
    }

    fn insert_node(&mut self, node: Node<T>) -> SlabToken {
        let raw = self.slab.insert(node);
        SlabToken::from_raw(raw)
    }

    fn get_node(&self, token: SlabToken) -> &Node<T> {
        unsafe { self.slab.get(token.to_raw()).strict_unwrap_unchecked() }
    }

    fn get_node_mut(&mut self, token: SlabToken) -> &mut Node<T> {
        unsafe { self.slab.get_mut(token.to_raw()).strict_unwrap_unchecked() }
    }

    fn remove_node(&mut self, token: SlabToken) -> Node<T> {
        self.slab.remove(token.to_raw())
    }
}

/// Item reference iterator of the double linked list.
pub struct SlabLinkedListIter<'a, T> {
    token: Option<SlabToken>,
    list: &'a SlabLinkedList<T>,
}

impl<'a, T> SlabLinkedListIter<'a, T> {
    /// Check if the iter is in a valid position.
    pub fn is_valid(&self) -> bool {
        self.token.is_some()
    }

    /// Get the item of the current position.
    pub fn data(&self) -> Option<&'a T> {
        self.token.map(|token| &self.list.get_node(token).data)
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        match self.token {
            Some(token) => self.token = self.list.get_node(token).next,
            None => self.token = self.list.head,
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        match self.token {
            Some(token) => self.token = self.list.get_node(token).prev,
            None => self.token = self.list.tail,
        }
    }

    /// Move to head.
    pub fn front(&mut self) {
        self.token = self.list.head;
    }

    /// Move to head.
    pub fn back(&mut self) {
        self.token = self.list.tail;
    }

    /// Check if the iterator is in the first position of the double linked list.
    pub fn is_front(&self) -> bool {
        self.token == self.list.head
    }

    /// Check if the iterator is in the last position of the double linked list.
    pub fn is_back(&self) -> bool {
        self.token == self.list.tail
    }
}

/// Item mutable reference iterator of the double linked list.
pub struct SlabLinkedListIterMut<'a, T> {
    token: Option<SlabToken>,
    list: &'a mut SlabLinkedList<T>,
}

impl<'a, T> SlabLinkedListIterMut<'a, T> {
    /// Check if the iter is in a valid position.
    pub fn is_valid(&self) -> bool {
        self.token.is_some()
    }

    /// Get the item reference of the current position.
    pub fn data(&self) -> Option<&'a T> {
        self.token
            .map(|token| &self.list.get_node(token).data)
            // Need an unbound lifetime to get 'a
            .map(|data| unsafe { &*(data as *const _) })
    }

    /// Get the item mutable reference of the current position.
    pub fn data_mut(&mut self) -> Option<&'a mut T> {
        self.token
            .map(|token| &mut self.list.get_node_mut(token).data)
            // Need an unbound lifetime to get 'a
            .map(|data| unsafe { &mut *(data as *mut _) })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        match self.token {
            Some(token) => self.token = self.list.get_node(token).next,
            None => self.token = self.list.head,
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        match self.token {
            Some(token) => self.token = self.list.get_node(token).prev,
            None => self.token = self.list.tail,
        }
    }

    /// Move to front.
    pub fn front(&mut self) {
        self.token = self.list.head;
    }

    /// Move to back.
    pub fn back(&mut self) {
        self.token = self.list.tail;
    }

    /// Removes the current item from [`Dlist`] and move next.
    pub fn remove(&mut self) -> Option<T> {
        let token = self.token?;

        let mut node = self.list.remove_node(token);

        // fix head and tail if node is either of that
        let prev = node.prev;
        let next = node.next;
        if Some(token) == self.list.head {
            self.list.head = next;
        }
        if Some(token) == self.list.tail {
            self.list.tail = prev;
        }

        // fix the next and prev ptrs of the node before and after this
        if let Some(prev) = prev {
            self.list.get_node_mut(prev).next = next;
        }
        if let Some(next) = next {
            self.list.get_node_mut(next).prev = prev;
        }

        node.next = None;
        node.prev = None;
        self.list.len -= 1;

        self.token = next;

        Some(node.data)
    }

    /// Insert an item before the current one.
    ///
    /// If iter is on null, insert to tail.
    pub fn insert_before(&mut self, data: T) {
        let token_new = self.list.insert_node(Node::new(data));

        match self.token {
            Some(token) => self.link_before(token_new, token),
            None => {
                self.link_between(token_new, self.list.tail, None);
                self.list.tail = Some(token_new);
            }
        }

        if self.list.head == self.token {
            self.list.head = Some(token_new);
        }

        self.list.len += 1;
    }

    /// Insert an item after the current one.
    ///
    /// If iter is on null, insert to head.
    pub fn insert_after(&mut self, data: T) {
        let token_new = self.list.insert_node(Node::new(data));

        match self.token {
            Some(token) => self.link_after(token_new, token),
            None => {
                self.link_between(token_new, None, self.list.head);
                self.list.head = Some(token_new);
            }
        }

        if self.list.tail == self.token {
            self.list.tail = Some(token_new);
        }

        self.list.len += 1;
    }

    fn link_before(&mut self, token: SlabToken, next: SlabToken) {
        self.link_between(token, self.list.get_node(next).prev, Some(next));
    }

    fn link_after(&mut self, token: SlabToken, prev: SlabToken) {
        self.link_between(token, Some(prev), self.list.get_node(prev).next);
    }

    fn link_between(&mut self, token: SlabToken, prev: Option<SlabToken>, next: Option<SlabToken>) {
        if let Some(prev) = prev {
            self.list.get_node_mut(prev).next = Some(token);
        }
        if let Some(next) = next {
            self.list.get_node_mut(next).prev = Some(token);
        }

        let node = self.list.get_node_mut(token);
        node.prev = prev;
        node.next = next;
    }

    /// Check if the iterator is in the first position of the double linked list.
    pub fn is_front(&self) -> bool {
        self.token == self.list.head
    }

    /// Check if the iterator is in the last position of the double linked list.
    pub fn is_back(&self) -> bool {
        self.token == self.list.tail
    }
}

impl<'a, T> Iterator for SlabLinkedListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        self.data()
    }
}

impl<'a, T> Iterator for SlabLinkedListIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        self.data_mut()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_repr() {
        assert_eq!(
            std::mem::size_of::<SlabToken>(),
            std::mem::size_of::<Option<SlabToken>>()
        );
    }

    #[test]
    fn test_dlist_simple() {
        let mut l = SlabLinkedList::new();

        l.push_back(2);
        l.push_front(1);
        l.push_back(3);

        let v = l.iter().copied().collect_vec();
        assert_eq!(v, vec![1, 2, 3]);
        assert_eq!(l.len(), 3);

        let mut iter = l.iter_mut();
        iter.next();
        iter.next();
        assert_eq!(*iter.data().unwrap(), 2);

        let d2 = iter.remove().unwrap();
        assert_eq!(d2, 2);
        assert_eq!(*iter.data().unwrap(), 3);
        let v = l.iter().copied().collect_vec();
        assert_eq!(v, vec![1, 3]);
        assert_eq!(l.len(), 2);

        let d3 = l.pop_back().unwrap();
        assert_eq!(d3, 3);
        let d1 = l.pop_front().unwrap();
        assert_eq!(d1, 1);
        let v = l.iter().copied().collect_vec();
        assert_eq!(v, vec![]);
        assert_eq!(l.len(), 0);
        assert!(l.is_empty());

        assert!(l.pop_front().is_none());
    }
}
