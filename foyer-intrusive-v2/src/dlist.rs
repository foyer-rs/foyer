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

//! An intrusive double linked list implementation.

use std::ptr::NonNull;

use foyer_common::{assert::OptionExt, strict_assert};

use crate::adapter::{Adapter, Link};

/// The link for the intrusive double linked list.
#[derive(Debug, Default)]
pub struct DlistLink {
    prev: Option<NonNull<DlistLink>>,
    next: Option<NonNull<DlistLink>>,
    is_linked: bool,
}

impl DlistLink {
    /// Get the `NonNull` pointer of the link.
    pub fn raw(&mut self) -> NonNull<DlistLink> {
        unsafe { NonNull::new_unchecked(self as *mut _) }
    }

    /// Get the pointer of the prev link.
    pub fn prev(&self) -> Option<NonNull<DlistLink>> {
        self.prev
    }

    /// Get the pointer of the next link.
    pub fn next(&self) -> Option<NonNull<DlistLink>> {
        self.next
    }
}

unsafe impl Send for DlistLink {}
unsafe impl Sync for DlistLink {}

impl Link for DlistLink {
    fn is_linked(&self) -> bool {
        self.is_linked
    }
}

/// Intrusive double linked list.
#[derive(Debug)]
pub struct Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    head: Option<NonNull<DlistLink>>,
    tail: Option<NonNull<DlistLink>>,

    len: usize,

    adapter: A,
}

unsafe impl<A> Send for Dlist<A> where A: Adapter<Link = DlistLink> {}
unsafe impl<A> Sync for Dlist<A> where A: Adapter<Link = DlistLink> {}

impl<A> Drop for Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    fn drop(&mut self) {
        let mut iter = self.iter_mut();
        iter.front();
        while iter.is_valid() {
            iter.remove();
        }
        assert!(self.is_empty());
    }
}

impl<A> Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    /// Create a new intrusive double linked list.
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,

            adapter: A::new(),
        }
    }

    /// Get the reference of the first item of the intrusive double linked list.
    pub fn front(&self) -> Option<&A::Item> {
        unsafe { self.head.map(|link| self.adapter.link2ptr(link).as_ref()) }
    }

    /// Get the reference of the last item of the intrusive double linked list.
    pub fn back(&self) -> Option<&A::Item> {
        unsafe { self.tail.map(|link| self.adapter.link2ptr(link).as_ref()) }
    }

    /// Get the mutable reference of the first item of the intrusive double linked list.
    pub fn front_mut(&mut self) -> Option<&mut A::Item> {
        unsafe { self.head.map(|link| self.adapter.link2ptr(link).as_mut()) }
    }

    /// Get the mutable reference of the last item of the intrusive double linked list.
    pub fn back_mut(&mut self) -> Option<&mut A::Item> {
        unsafe { self.tail.map(|link| self.adapter.link2ptr(link).as_mut()) }
    }

    /// Push an item to the first position of the intrusive double linked list.
    pub fn push_front(&mut self, ptr: NonNull<A::Item>) {
        self.iter_mut().insert_after(ptr);
    }

    /// Push an item to the last position of the intrusive double linked list.
    pub fn push_back(&mut self, ptr: NonNull<A::Item>) {
        self.iter_mut().insert_before(ptr);
    }

    /// Pop an item from the first position of the intrusive double linked list.
    pub fn pop_front(&mut self) -> Option<NonNull<A::Item>> {
        let mut iter = self.iter_mut();
        iter.next();
        iter.remove()
    }

    /// Pop an item from the last position of the intrusive double linked list.
    pub fn pop_back(&mut self) -> Option<NonNull<A::Item>> {
        let mut iter = self.iter_mut();
        iter.prev();
        iter.remove()
    }

    /// Get the item reference iterator of the intrusive double linked list.
    pub fn iter(&self) -> DlistIter<'_, A> {
        DlistIter {
            link: None,
            dlist: self,
        }
    }

    /// Get the item mutable reference iterator of the intrusive double linked list.
    pub fn iter_mut(&mut self) -> DlistIterMut<'_, A> {
        DlistIterMut {
            link: None,
            dlist: self,
        }
    }

    /// Get the length of the intrusive double linked list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the intrusive double linked list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove an ptr.
    ///
    /// # Safety
    ///
    /// `ptr` MUST be in this [`Dlist`].
    pub unsafe fn remove(&mut self, ptr: NonNull<A::Item>) -> NonNull<A::Item> {
        let mut iter = self.iter_mut_with_ptr(ptr);
        strict_assert!(iter.is_valid());
        iter.remove().strict_unwrap_unchecked()
    }

    /// Create mutable iterator directly on ptr.
    ///
    /// # Safety
    ///
    /// `ptr` MUST be in this [`Dlist`].
    pub unsafe fn iter_mut_with_ptr(&mut self, ptr: NonNull<A::Item>) -> DlistIterMut<'_, A> {
        let link = self.adapter.ptr2link(ptr);
        DlistIterMut {
            link: Some(link),
            dlist: self,
        }
    }

    /// Create immutable iterator directly on ptr.
    ///
    /// # Safety
    ///
    /// `ptr` MUST be in this [`Dlist`].
    pub unsafe fn iter_from_with_ptr(&self, ptr: NonNull<A::Item>) -> DlistIter<'_, A> {
        let link = self.adapter.ptr2link(ptr);
        DlistIter {
            link: Some(link),
            dlist: self,
        }
    }

    /// Get the intrusive adapter of the double linked list.
    pub fn adapter(&self) -> &A {
        &self.adapter
    }
}

/// Item reference iterator of the intrusive double linked list.
pub struct DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a Dlist<A>,
}

impl<'a, A> DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    /// Check if the iter is in a valid position.
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    /// Get the item of the current position.
    pub fn item(&self) -> Option<&A::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2ptr(link).as_ref() })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().next,
                None => self.link = self.dlist.head,
            }
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().prev,
                None => self.link = self.dlist.tail,
            }
        }
    }

    /// Move to head.
    pub fn front(&mut self) {
        self.link = self.dlist.head;
    }

    /// Move to head.
    pub fn back(&mut self) {
        self.link = self.dlist.tail;
    }

    /// Check if the iterator is in the first position of the intrusive double linked list.
    pub fn is_front(&self) -> bool {
        self.link == self.dlist.head
    }

    /// Check if the iterator is in the last position of the intrusive double linked list.
    pub fn is_back(&self) -> bool {
        self.link == self.dlist.tail
    }
}

/// Item mutable reference iterator of the intrusive double linked list.
pub struct DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a mut Dlist<A>,
}

impl<'a, A> DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    /// Check if the iter is in a valid position.
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    /// Get the item reference of the current position.
    pub fn item(&self) -> Option<&A::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2ptr(link).as_ref() })
    }

    /// Get the item mutable reference of the current position.
    pub fn item_mut(&mut self) -> Option<&mut A::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2ptr(link).as_mut() })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().next,
                None => self.link = self.dlist.head,
            }
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().prev,
                None => self.link = self.dlist.tail,
            }
        }
    }

    /// Move to front.
    pub fn front(&mut self) {
        self.link = self.dlist.head;
    }

    /// Move to back.
    pub fn back(&mut self) {
        self.link = self.dlist.tail;
    }

    /// Removes the current item from [`Dlist`] and move next.
    pub fn remove(&mut self) -> Option<NonNull<A::Item>> {
        unsafe {
            if !self.is_valid() {
                return None;
            }

            strict_assert!(self.is_valid());
            let mut link = self.link.strict_unwrap_unchecked();
            let ptr = self.dlist.adapter.link2ptr(link);

            // fix head and tail if node is either of that
            let mut prev = link.as_ref().prev;
            let mut next = link.as_ref().next;
            if Some(link) == self.dlist.head {
                self.dlist.head = next;
            }
            if Some(link) == self.dlist.tail {
                self.dlist.tail = prev;
            }

            // fix the next and prev ptrs of the node before and after this
            if let Some(prev) = &mut prev {
                prev.as_mut().next = next;
            }
            if let Some(next) = &mut next {
                next.as_mut().prev = prev;
            }

            link.as_mut().next = None;
            link.as_mut().prev = None;
            link.as_mut().is_linked = false;

            self.dlist.len -= 1;

            self.link = next;

            Some(ptr)
        }
    }

    /// Link a new ptr before the current one.
    ///
    /// If iter is on null, link to tail.
    pub fn insert_before(&mut self, ptr: NonNull<A::Item>) {
        unsafe {
            let mut link_new = self.dlist.adapter.ptr2link(ptr);
            assert!(!link_new.as_ref().is_linked());

            match self.link {
                Some(link) => self.link_before(link_new, link),
                None => {
                    self.link_between(link_new, self.dlist.tail, None);
                    self.dlist.tail = Some(link_new);
                }
            }

            if self.dlist.head == self.link {
                self.dlist.head = Some(link_new);
            }

            link_new.as_mut().is_linked = true;

            self.dlist.len += 1;
        }
    }

    /// Link a new ptr after the current one.
    ///
    /// If iter is on null, link to head.
    pub fn insert_after(&mut self, ptr: NonNull<A::Item>) {
        unsafe {
            let mut link_new = self.dlist.adapter.ptr2link(ptr);
            assert!(!link_new.as_ref().is_linked());

            match self.link {
                Some(link) => self.link_after(link_new, link),
                None => {
                    self.link_between(link_new, None, self.dlist.head);
                    self.dlist.head = Some(link_new);
                }
            }

            if self.dlist.tail == self.link {
                self.dlist.tail = Some(link_new);
            }

            link_new.as_mut().is_linked = true;

            self.dlist.len += 1;
        }
    }

    unsafe fn link_before(&mut self, link: NonNull<A::Link>, next: NonNull<A::Link>) {
        self.link_between(link, next.as_ref().prev, Some(next));
    }

    unsafe fn link_after(&mut self, link: NonNull<A::Link>, prev: NonNull<A::Link>) {
        self.link_between(link, Some(prev), prev.as_ref().next);
    }

    unsafe fn link_between(
        &mut self,
        mut link: NonNull<A::Link>,
        mut prev: Option<NonNull<A::Link>>,
        mut next: Option<NonNull<A::Link>>,
    ) {
        if let Some(prev) = &mut prev {
            prev.as_mut().next = Some(link);
        }
        if let Some(next) = &mut next {
            next.as_mut().prev = Some(link);
        }
        link.as_mut().prev = prev;
        link.as_mut().next = next;
    }

    /// Check if the iterator is in the first position of the intrusive double linked list.
    pub fn is_front(&self) -> bool {
        self.link == self.dlist.head
    }

    /// Check if the iterator is in the last position of the intrusive double linked list.
    pub fn is_back(&self) -> bool {
        self.link == self.dlist.tail
    }
}

impl<'a, A> Iterator for DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    type Item = &'a A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => Some(unsafe { self.dlist.adapter.link2ptr(link).as_ref() }),
            None => None,
        }
    }
}

impl<'a, A> Iterator for DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    type Item = &'a mut A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => Some(unsafe { self.dlist.adapter.link2ptr(link).as_mut() }),
            None => None,
        }
    }
}

// TODO(MrCroxx): Need more tests.

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::intrusive_adapter;

    #[derive(Debug)]
    struct DlistItem {
        link: DlistLink,
        val: u64,
    }

    impl DlistItem {
        fn new(val: u64) -> Self {
            Self {
                link: DlistLink::default(),
                val,
            }
        }
    }

    #[derive(Debug, Default)]
    struct DlistAdapter;

    unsafe impl Adapter for DlistAdapter {
        type Item = DlistItem;
        type Link = DlistLink;

        fn new() -> Self {
            Self
        }

        unsafe fn link2ptr(&self, link: NonNull<Self::Link>) -> NonNull<Self::Item> {
            NonNull::new_unchecked(crate::container_of!(link.as_ptr(), DlistItem, link))
        }

        unsafe fn ptr2link(&self, item: NonNull<Self::Item>) -> NonNull<Self::Link> {
            NonNull::new_unchecked((item.as_ptr() as *const u8).add(std::mem::offset_of!(DlistItem, link)) as *mut _)
        }
    }

    intrusive_adapter! { DlistArcAdapter = DlistItem { link => DlistLink } }

    #[test]
    fn test_dlist_simple() {
        let mut l = Dlist::<DlistAdapter>::new();

        l.push_back(unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(DlistItem::new(2)))) });
        l.push_front(unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(DlistItem::new(1)))) });
        l.push_back(unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(DlistItem::new(3)))) });

        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 2, 3]);
        assert_eq!(l.len(), 3);

        let mut iter = l.iter_mut();
        iter.next();
        iter.next();
        assert_eq!(iter.item().unwrap().val, 2);
        let p2 = iter.remove();
        let i2 = unsafe { Box::from_raw(p2.unwrap().as_ptr()) };
        assert_eq!(i2.val, 2);
        assert_eq!(iter.item().unwrap().val, 3);
        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 3]);
        assert_eq!(l.len(), 2);

        let p3 = l.pop_back();
        let i3 = unsafe { Box::from_raw(p3.unwrap().as_ptr()) };
        assert_eq!(i3.val, 3);
        let p1 = l.pop_front();
        let i1 = unsafe { Box::from_raw(p1.unwrap().as_ptr()) };
        assert_eq!(i1.val, 1);
        assert!(l.pop_front().is_none());
        assert_eq!(l.len(), 0);
    }
}
