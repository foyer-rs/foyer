//  Copyright 2023 MrCroxx
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

use std::marker::PhantomData;
use std::ptr::NonNull;

/// An entry in an intrusive double linked list
#[derive(Default, Debug)]
pub struct Entry {
    /// The prev entry ptr in the double linked list.
    prev: Option<NonNull<Entry>>,

    /// The next entry ptr in the double linked list.
    next: Option<NonNull<Entry>>,
}

pub trait Adapter<E> {
    /// entry ptr to element ptr
    fn en2el(_: NonNull<Entry>) -> NonNull<E>;

    /// element ptr to entry ptr
    fn el2en(_: NonNull<E>) -> NonNull<Entry>;
}

#[macro_export]
macro_rules! intrusive_dlist {
    (
        $element:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt)* )? ),+ >)?, $entry:ident, $adapter:ident
    ) => {
        struct $adapter;

        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? $crate::collections::dlist::Adapter<$element $(< $( $lt ),+ >)?> for $adapter  {
            fn en2el(entry: NonNull<Entry>) -> NonNull<$element $(< $( $lt ),+ >)?> {
                unsafe {
                    let ptr = (entry.as_ptr() as *mut u8)
                        .sub(memoffset::offset_of!($element $(< $( $lt ),+ >)?, $entry))
                        .cast::<$element $(< $( $lt ),+ >)?>();
                    NonNull::new_unchecked(ptr)
                }
            }

            fn el2en(element: NonNull<$element $(< $( $lt ),+ >)?>) -> NonNull<Entry> {
                unsafe {
                    let ptr = (element.as_ptr() as *mut u8)
                        .add(memoffset::offset_of!($element $(< $( $lt ),+ >)?, $entry))
                        .cast::<Entry>();
                    NonNull::new_unchecked(ptr)
                }
            }
        }
    };
}

/// TODO: write docs
#[derive(Debug)]
pub struct DList<E, A: Adapter<E>> {
    /// head ptr of the double linked list
    head: Option<NonNull<Entry>>,

    /// tail ptr of the double linked list
    tail: Option<NonNull<Entry>>,

    /// length of the double linked list
    len: usize,

    _marker: PhantomData<(E, A)>,
}

impl<E, A: Adapter<E>> Default for DList<E, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A: Adapter<E>> DList<E, A> {
    /// create an empty intrusive double linklist
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
            _marker: PhantomData::default(),
        }
    }

    /// link a dangling element to the list at head
    ///
    /// # Safety
    ///
    /// `element` must not be in the list
    pub unsafe fn link_at_head(&mut self, element: NonNull<E>) {
        let mut entry = A::el2en(element);
        assert_ne!(Some(entry), self.head);

        entry.as_mut().next = self.head;
        entry.as_mut().prev = None;

        // fix the prev ptr of head
        if let Some(head) = &mut self.head {
            head.as_mut().prev = Some(entry);
        }
        self.head = Some(entry);

        if self.tail.is_none() {
            self.tail = Some(entry)
        }

        self.len += 1;
    }

    /// link a dangling element to the list at tail
    ///
    /// # Safety
    ///
    /// `element` must not be in the list
    pub unsafe fn link_at_tail(&mut self, element: NonNull<E>) {
        let mut entry = A::el2en(element);
        assert_ne!(Some(entry), self.tail);

        entry.as_mut().next = None;
        entry.as_mut().prev = self.tail;

        // fix the prev ptr of tail
        if let Some(tail) = &mut self.tail {
            tail.as_mut().next = Some(entry);
        }
        self.tail = Some(entry);

        if self.head.is_none() {
            self.head = Some(entry)
        }

        self.len += 1;
    }

    /// link a dangling element to the list before `next_element`
    ///
    /// # Safety
    ///
    /// `element` must not be in the list
    /// `next_element` must be in the list
    pub unsafe fn link_before(&mut self, next_element: NonNull<E>, element: NonNull<E>) {
        let mut next_entry = A::el2en(next_element);
        let mut entry = A::el2en(element);
        assert_ne!(next_entry, entry);

        let mut prev = next_entry.as_mut().prev;
        assert_ne!(prev, Some(entry));

        entry.as_mut().prev = prev;
        match &mut prev {
            Some(prev) => prev.as_mut().next = Some(entry),
            None => self.head = Some(entry),
        }

        next_entry.as_mut().prev = Some(entry);
        entry.as_mut().next = Some(next_entry);

        self.len += 1;
    }

    /// unlink an element from the list
    ///
    /// # Safety
    ///
    /// `element` must be in the list
    pub unsafe fn unlink(&mut self, element: NonNull<E>) {
        assert!(self.len > 0);

        let mut entry = A::el2en(element);

        // fix head and tail if node is either of that
        let mut prev = entry.as_mut().prev;
        let mut next = entry.as_mut().next;
        if Some(entry) == self.head {
            self.head = next;
        }
        if Some(entry) == self.tail {
            self.tail = prev;
        }

        // fix the next and prev ptrs of the node before and after this
        if let Some(prev) = &mut prev {
            prev.as_mut().next = next;
        }
        if let Some(next) = &mut next {
            next.as_mut().prev = prev;
        }

        self.len -= 1;
    }

    /// remove an element from the list and cleanup its pointers
    ///
    /// # Safety
    ///
    /// `element` must be in the list
    pub unsafe fn remove(&mut self, element: NonNull<E>) {
        self.unlink(element);
        let mut entry = A::el2en(element);
        entry.as_mut().next = None;
        entry.as_mut().prev = None;
    }

    /// replace an element with an dangling element
    ///
    /// # Safety
    ///
    /// `old_element` must be in the list
    /// `new_element` must not be in the list
    pub unsafe fn replace(&mut self, old_element: NonNull<E>, new_element: NonNull<E>) {
        let mut old_entry = A::el2en(old_element);
        let mut new_entry = A::el2en(new_element);

        // update head and tail links if needed
        if Some(old_entry) == self.head {
            self.head = Some(new_entry);
        }
        if Some(old_entry) == self.tail {
            self.tail = Some(new_entry);
        }

        let mut prev = old_entry.as_mut().prev;
        let mut next = old_entry.as_mut().next;

        // make the prev and next entry point to the new entry
        if let Some(prev) = &mut prev {
            prev.as_mut().next = Some(new_entry);
        }
        if let Some(next) = &mut next {
            next.as_mut().prev = Some(new_entry);
        }

        // make the new entry point to the prev and next entry
        new_entry.as_mut().prev = prev;
        new_entry.as_mut().next = next;

        // cleanup the old node
        old_entry.as_mut().prev = None;
        old_entry.as_mut().next = None;
    }

    /// move an element to the head of the list
    ///
    /// # Safety
    ///
    /// `element` must be in the list
    pub unsafe fn move_to_head(&mut self, element: NonNull<E>) {
        let entry = A::el2en(element);
        if Some(entry) == self.head {
            return;
        }
        self.unlink(element);
        self.link_at_head(element);
    }

    /// move an element to the tail of the list
    ///
    /// # Safety
    ///
    /// `element` must be in the list
    pub unsafe fn move_to_tail(&mut self, element: NonNull<E>) {
        let entry = A::el2en(element);
        if Some(entry) == self.tail {
            return;
        }
        self.unlink(element);
        self.link_at_tail(element);
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// create a iterator of the list from head
    ///
    /// # Safety
    ///
    /// there is no guarantee that the element cannot be modified while iterating
    pub unsafe fn iter(&mut self) -> Iter<'_, E, A> {
        let entry = self.head;
        Iter { dlist: self, entry }
    }
}

pub struct Iter<'a, E, A: Adapter<E>> {
    dlist: &'a mut DList<E, A>,
    entry: Option<NonNull<Entry>>,
}

impl<'a, E, A: Adapter<E>> Iter<'a, E, A> {
    /// get element ptr
    pub fn element(&self) -> Option<NonNull<E>> {
        self.entry.map(|entry| A::en2el(entry))
    }

    /// move to next
    ///
    /// # Safety
    ///
    /// the iterator must be in a valid position
    pub unsafe fn next(&mut self) {
        self.entry = self.entry.unwrap().as_ref().next
    }

    /// move to prev
    ///
    /// # Safety
    ///
    /// the iterator must be in a valid position
    pub unsafe fn prev(&mut self) {
        self.entry = self.entry.unwrap().as_ref().prev
    }

    /// move to head
    pub fn head(&mut self) {
        self.entry = self.dlist.head
    }

    /// move to tail
    pub fn tail(&mut self) {
        self.entry = self.dlist.tail
    }
}

impl<'a, E, A: Adapter<E>> Iterator for Iter<'a, E, A> {
    type Item = NonNull<E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.element() {
            Some(element) => {
                unsafe { self.next() };
                Some(element)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use itertools::Itertools;

    #[derive(Debug)]
    struct SingleElement {
        e: Entry,
        data: usize,
    }

    impl SingleElement {
        fn new(data: usize) -> Self {
            Self {
                e: Entry::default(),
                data,
            }
        }
    }

    impl Drop for SingleElement {
        fn drop(&mut self) {
            self.data = 0
        }
    }

    #[derive(Debug)]
    struct DoubleElement {
        e1: Entry,
        e2: Entry,
        data: usize,
    }

    impl DoubleElement {
        fn new(data: usize) -> Self {
            Self {
                e1: Entry::default(),
                e2: Entry::default(),
                data,
            }
        }
    }

    impl Drop for DoubleElement {
        fn drop(&mut self) {
            self.data = 0
        }
    }

    intrusive_dlist! {SingleElement, e, SingleElementAdapter}
    intrusive_dlist! {DoubleElement, e1, DoubleElementAdapter1}
    intrusive_dlist! {DoubleElement, e2, DEA2DoubleElementAdapter2}

    #[test]
    fn test_se_simple() {
        unsafe {
            let mut l: DList<SingleElement, SingleElementAdapter> = DList::new();

            let mut es = vec![
                SingleElement::new(1),
                SingleElement::new(2),
                SingleElement::new(3),
            ];

            es.iter_mut()
                .for_each(|e| l.link_at_tail(NonNull::new_unchecked(e as *mut _)));

            assert_eq!(l.len(), 3);
            assert_eq!(
                vec![1, 2, 3],
                l.iter().map(|e| e.as_ref().data).collect_vec()
            );

            drop(es);
        }
    }

    #[test]
    fn test_de_simple() {
        unsafe {
            let mut l1: DList<DoubleElement, DoubleElementAdapter1> = DList::new();
            let mut l2: DList<DoubleElement, DEA2DoubleElementAdapter2> = DList::new();

            let mut es = vec![
                DoubleElement::new(1),
                DoubleElement::new(2),
                DoubleElement::new(3),
            ];
            es.iter_mut()
                .for_each(|e| l1.link_at_tail(NonNull::new_unchecked(e as *mut _)));
            es.iter_mut()
                .for_each(|e| l2.link_at_head(NonNull::new_unchecked(e as *mut _)));

            assert_eq!(l1.len(), 3);
            assert_eq!(l2.len(), 3);

            assert_eq!(
                vec![1, 2, 3],
                l1.iter().map(|e| e.as_ref().data).collect_vec()
            );
            assert_eq!(
                vec![3, 2, 1],
                l2.iter().map(|e| e.as_ref().data).collect_vec()
            );

            drop(es);
        }
    }

    #[test]
    fn test_link_before() {
        unsafe {
            let mut l: DList<SingleElement, SingleElementAdapter> = DList::new();

            let mut es = vec![
                SingleElement::new(1),
                SingleElement::new(2),
                SingleElement::new(3),
            ];

            es.iter_mut()
                .for_each(|e| l.link_at_tail(NonNull::new_unchecked(e as *mut _)));

            let mut e = SingleElement::new(4);
            l.link_before(
                NonNull::new_unchecked(&mut es[2] as *mut _),
                NonNull::new_unchecked(&mut e as *mut _),
            );

            assert_eq!(l.len(), 4);
            assert_eq!(
                vec![1, 2, 4, 3],
                l.iter().map(|e| e.as_ref().data).collect_vec()
            );

            drop(e);
            drop(es);
        }
    }

    #[test]
    fn test_remove() {
        unsafe {
            let mut l: DList<SingleElement, SingleElementAdapter> = DList::new();

            let mut es = vec![
                SingleElement::new(1),
                SingleElement::new(2),
                SingleElement::new(3),
            ];

            es.iter_mut()
                .for_each(|e| l.link_at_tail(NonNull::new_unchecked(e as *mut _)));

            l.remove(NonNull::new_unchecked(&mut es[1] as *mut _));

            assert_eq!(l.len(), 2);
            assert_eq!(vec![1, 3], l.iter().map(|e| e.as_ref().data).collect_vec());

            drop(es);
        }
    }

    #[test]
    fn test_link_replace() {
        unsafe {
            let mut l: DList<SingleElement, SingleElementAdapter> = DList::new();

            let mut es = vec![
                SingleElement::new(1),
                SingleElement::new(2),
                SingleElement::new(3),
            ];

            es.iter_mut()
                .for_each(|e| l.link_at_tail(NonNull::new_unchecked(e as *mut _)));

            let mut e = SingleElement::new(4);
            l.replace(
                NonNull::new_unchecked(&mut es[1] as *mut _),
                NonNull::new_unchecked(&mut e as *mut _),
            );

            assert_eq!(l.len(), 3);
            assert_eq!(
                vec![1, 4, 3],
                l.iter().map(|e| e.as_ref().data).collect_vec()
            );

            drop(e);
            drop(es);
        }
    }

    #[test]
    fn test_move_to_head() {
        unsafe {
            let mut l: DList<SingleElement, SingleElementAdapter> = DList::new();

            let mut es = vec![
                SingleElement::new(1),
                SingleElement::new(2),
                SingleElement::new(3),
            ];

            es.iter_mut()
                .for_each(|e| l.link_at_tail(NonNull::new_unchecked(e as *mut _)));

            l.move_to_head(NonNull::new_unchecked(&mut es[1] as *mut _));

            assert_eq!(l.len(), 3);
            assert_eq!(
                vec![2, 1, 3],
                l.iter().map(|e| e.as_ref().data).collect_vec()
            );

            drop(es);
        }
    }
}
