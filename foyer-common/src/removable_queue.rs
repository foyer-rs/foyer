//  Copyright 2024 MrCroxx
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

use std::mem::ManuallyDrop;

#[derive(Debug, PartialEq, Eq)]
pub struct Token(usize);

/// A FIFO queue that supports random lazy item removal.
pub struct RemovableQueue<T> {
    /// Heap-allocated ring queue buffer.
    queue: Box<[Option<T>]>,

    /// Pop point.
    head: usize,
    /// Push point.
    tail: usize,

    /// Actually element count.
    len: usize,
    /// Heap-allocated ring queue buffer capacity.
    capacity: usize,

    /// The global offset of the current layout. Only updates on growth.
    token: usize,
}

impl<T> Default for RemovableQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> RemovableQueue<T> {
    pub const DEFAULT_CAPACITY: usize = 16;

    /// Create an empty [`RemovableQueue`] with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Create an empty [`RemovableQueue`] with given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Self::queue(capacity),

            head: 0,
            tail: 0,

            len: 0,
            capacity,

            token: 0,
        }
    }

    /// Push an element to the tail of the queue.
    ///
    /// Returns a token that can be used for randomly removal.
    pub fn push(&mut self, elem: T) -> Token {
        if self.usage() == self.capacity {
            self.grow_to(self.capacity * 2);
        }

        debug_assert!(self.usage() < self.capacity());

        let pos = self.tail % self.capacity;
        debug_assert!(self.queue[pos].is_none());
        self.queue[pos] = Some(elem);

        let token = self.token + self.tail;
        self.tail += 1;
        self.len += 1;

        Token(token)
    }

    /// Pop an element from the head of the queue.
    pub fn pop(&mut self) -> Option<T> {
        let mut res = None;

        while res.is_none() && self.usage() > 0 {
            let pos = self.head % self.capacity;
            res = self.queue[pos].take();
            self.head += 1;
        }

        while self.usage() > 0
            && let pos = self.head % self.capacity
            && self.queue[pos].is_none()
        {
            self.head += 1;
        }

        if res.is_some() {
            self.len -= 1;
        }

        res
    }

    /// Randonly remove the element with the given `token` from the queue.
    pub fn remove(&mut self, token: Token) -> Option<T> {
        if token.0 < self.token + self.head || token.0 >= self.token + self.tail {
            return None;
        }
        let pos = (token.0 - self.token) % self.capacity;
        self.len -= 1;
        self.queue[pos].take()
    }

    /// Remove and return all the elements from the queue.
    pub fn clear(&mut self) -> Vec<T> {
        let mut res = Vec::with_capacity(self.len);
        for pos in self.head..self.tail {
            let pos = pos % self.capacity;
            if let Some(elem) = self.queue[pos].take() {
                res.push(elem);
            }
        }

        self.token += self.tail;
        self.head = 0;
        self.tail = 0;
        self.len = 0;

        res
    }

    /// Returns the actually element count.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if there is no element in the queue.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the occupied slots of the queue.
    #[inline(always)]
    pub fn usage(&self) -> usize {
        self.tail - self.head
    }

    /// Returns the capacity of the queue.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Grow the capacity to the given capacity.
    ///
    /// If the current capacity is equal or greater than the given capacity, do nothing.
    fn grow_to(&mut self, capacity: usize) {
        unsafe {
            if capacity <= self.capacity {
                return;
            }

            let usage = self.usage();
            let mut queue = Self::queue(capacity);

            if usage > 0 {
                let phead = self.head % self.capacity;
                let ptail = self.tail % self.capacity;

                if phead < ptail {
                    //          ↓ phead            ↓ ptail
                    // * [ ][ ][x][x][x][x][x][x][ ][ ][ ][ ]
                    std::ptr::copy_nonoverlapping(
                        &self.queue[phead] as *const _,
                        &mut queue[0] as *mut _,
                        ptail - phead,
                    );
                } else {
                    // before:
                    //
                    //          ↓ ptail           ↓ phead
                    // * [o][o][ ][ ][ ][ ][ ][ ][x][x][x][x] (or full)
                    //
                    // after:
                    //
                    //    ↓ phead           ↓ ptail
                    // * [x][x][x][x][o][o][ ][ ][ ][ ][ ][ ]
                    std::ptr::copy_nonoverlapping(
                        &self.queue[phead] as *const _,
                        &mut queue[0] as *mut _,
                        self.capacity - phead,
                    );
                    std::ptr::copy_nonoverlapping(
                        &self.queue[0] as *const _,
                        &mut queue[self.capacity - phead] as *mut _,
                        ptail,
                    );
                }
            }

            self.token += self.head;

            std::mem::swap(&mut self.queue, &mut queue);
            self.capacity = capacity;
            self.head = 0;
            self.tail = usage;

            // Drop the slice, and prevent from dropping the moved elements.
            std::mem::transmute::<_, Box<[ManuallyDrop<Option<T>>]>>(queue);
        }
    }

    fn queue(capacity: usize) -> Box<[Option<T>]> {
        std::iter::repeat_with(|| None)
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_removable_queue() {
        let mut queue = RemovableQueue::with_capacity(4);
        assert_eq!(queue.capacity(), 4);
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.usage(), 0);

        assert_eq!(queue.push(0), Token(0));
        assert_eq!(queue.push(1), Token(1));
        assert_eq!(queue.push(2), Token(2));
        assert_eq!(queue.push(3), Token(3));
        assert_eq!(queue.capacity(), 4);
        assert_eq!(queue.len(), 4);
        assert_eq!(queue.usage(), 4);

        assert_eq!(queue.pop().unwrap(), 0);
        assert_eq!(queue.pop().unwrap(), 1);
        assert_eq!(queue.capacity(), 4);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.usage(), 2);

        assert_eq!(queue.push(4), Token(4));
        assert_eq!(queue.push(5), Token(5));
        assert_eq!(queue.push(6), Token(6));
        assert_eq!(queue.capacity(), 8);
        assert_eq!(queue.len(), 5);
        assert_eq!(queue.usage(), 5);

        assert_eq!(queue.remove(Token(3)), Some(3));
        assert_eq!(queue.remove(Token(4)), Some(4));
        assert_eq!(queue.remove(Token(5)), Some(5));
        assert_eq!(queue.capacity(), 8);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.usage(), 5);

        assert_eq!(queue.pop().unwrap(), 2);
        assert_eq!(queue.capacity(), 8);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.usage(), 1);

        assert_eq!(queue.clear(), vec![6]);
        assert_eq!(queue.capacity(), 8);
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.usage(), 0);
        assert!(queue.is_empty());

        for i in 0..8 {
            assert_eq!(queue.push(i), Token(i + 7));
        }
        assert_eq!(queue.capacity(), 8);
        assert_eq!(queue.len(), 8);
        assert_eq!(queue.usage(), 8);

        assert_eq!(queue.push(8), Token(8 + 7));
        assert_eq!(queue.capacity(), 16);
        assert_eq!(queue.len(), 9);
        assert_eq!(queue.usage(), 9);

        assert_eq!(queue.remove(Token(6)), None);

        assert_eq!(queue.pop(), Some(0));
        assert_eq!(queue.capacity(), 16);

        queue.grow_to(24);
        queue.grow_to(0);
        assert_eq!(queue.capacity(), 24);

        assert_eq!(queue.clear(), (1..9).collect_vec());
    }
}
