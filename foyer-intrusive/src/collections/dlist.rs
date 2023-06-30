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

use std::ptr::NonNull;

use crate::core::{
    adapter::{Adapter, Link},
    pointer::PointerOps,
};

#[derive(Debug, Default)]
pub struct DListLink {
    prev: Option<NonNull<DListLink>>,
    next: Option<NonNull<DListLink>>,
    is_linked: bool,
}

impl DListLink {
    pub fn raw(&self) -> NonNull<DListLink> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

unsafe impl Send for DListLink {}
unsafe impl Sync for DListLink {}

impl Link for DListLink {
    fn is_linked(&self) -> bool {
        self.is_linked
    }
}

#[derive(Debug)]
pub struct DList<A>
where
    A: Adapter<Link = DListLink>,
{
    head: Option<NonNull<DListLink>>,
    tail: Option<NonNull<DListLink>>,

    len: usize,

    adapter: A,
}

impl<A> DList<A>
where
    A: Adapter<Link = DListLink>,
{
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,

            adapter: A::new(),
        }
    }

    pub fn front(&self) -> Option<&<A::PointerOps as PointerOps>::Item> {
        unsafe {
            self.head
                .map(|link| self.adapter.link2item(link.as_ptr()))
                .map(|link| &*link)
        }
    }

    pub fn back(&self) -> Option<&<A::PointerOps as PointerOps>::Item> {
        unsafe {
            self.tail
                .map(|link| self.adapter.link2item(link.as_ptr()))
                .map(|link| &*link)
        }
    }

    pub fn push_front(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        self.iter_mut().insert_after(ptr);
    }

    pub fn push_back(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        self.iter_mut().insert_before(ptr);
    }

    pub fn pop_front(&mut self) -> Option<<A::PointerOps as PointerOps>::Pointer> {
        let mut iter = self.iter_mut();
        iter.next();
        iter.remove()
    }

    pub fn pop_back(&mut self) -> Option<<A::PointerOps as PointerOps>::Pointer> {
        let mut iter = self.iter_mut();
        iter.prev();
        iter.remove()
    }

    pub fn iter(&self) -> DListIter<'_, A> {
        DListIter {
            link: None,
            dlist: self,
        }
    }

    pub fn iter_mut(&mut self) -> DListIterMut<'_, A> {
        DListIterMut {
            link: None,
            dlist: self,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create mutable iterator directly on raw link.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`DList`].
    pub unsafe fn iter_mut_from_raw(&mut self, link: NonNull<DListLink>) -> DListIterMut<'_, A> {
        DListIterMut {
            link: Some(link),
            dlist: self,
        }
    }

    /// Create immutable iterator directly on raw link.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`DList`].
    pub unsafe fn iter_from_raw(&self, link: NonNull<DListLink>) -> DListIter<'_, A> {
        DListIter {
            link: Some(link),
            dlist: self,
        }
    }
}

pub struct DListIter<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a DList<A>,
}

impl<'a, A> DListIter<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    pub fn get(&self) -> Option<&<A::PointerOps as PointerOps>::Item> {
        self.link
            .map(|link| unsafe { &*self.dlist.adapter.link2item(link.as_ptr()) })
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
    pub fn head(&mut self) {
        self.link = self.dlist.head;
    }

    /// Move to head.
    pub fn tail(&mut self) {
        self.link = self.dlist.tail;
    }
}

pub struct DListIterMut<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a mut DList<A>,
}

impl<'a, A> DListIterMut<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    pub fn get(&self) -> Option<&<A::PointerOps as PointerOps>::Item> {
        self.link
            .map(|link| unsafe { &*self.dlist.adapter.link2item(link.as_ptr()) })
    }

    pub fn get_mut(&mut self) -> Option<&mut <A::PointerOps as PointerOps>::Item> {
        self.link
            .map(|link| unsafe { &mut *(self.dlist.adapter.link2item(link.as_ptr()) as *mut _) })
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

    /// Removes the current item from [`DList`] and move next.
    pub fn remove(&mut self) -> Option<<A::PointerOps as PointerOps>::Pointer> {
        unsafe {
            if !self.is_valid() {
                return None;
            }

            let mut link = self.link.unwrap();

            let item = self.dlist.adapter.link2item(link.as_ptr());
            let ptr = self.dlist.adapter.pointer_ops().from_raw(item);

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
    pub fn insert_before(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        unsafe {
            let item_new = self.dlist.adapter.pointer_ops().into_raw(ptr);
            let mut link_new =
                NonNull::new_unchecked(self.dlist.adapter.item2link(item_new) as *mut A::Link);
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
    pub fn insert_after(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        unsafe {
            let item_new = self.dlist.adapter.pointer_ops().into_raw(ptr);
            let mut link_new =
                NonNull::new_unchecked(self.dlist.adapter.item2link(item_new) as *mut A::Link);
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
}

impl<'a, A> Iterator for DListIter<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    type Item = &'a <A::PointerOps as PointerOps>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => Some(unsafe { &*(self.dlist.adapter.link2item(link.as_ptr())) }),
            None => None,
        }
    }
}

impl<'a, A> Iterator for DListIterMut<'a, A>
where
    A: Adapter<Link = DListLink>,
{
    type Item = &'a mut <A::PointerOps as PointerOps>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => {
                Some(unsafe { &mut *(self.dlist.adapter.link2item(link.as_ptr()) as *mut _) })
            }
            None => None,
        }
    }
}

// TODO(MrCroxx): Need more tests.

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::core::pointer::DefaultPointerOps;

    use super::*;

    #[derive(Debug)]
    struct DListItem {
        link: DListLink,
        val: u64,
    }

    impl DListItem {
        fn new(val: u64) -> Self {
            Self {
                link: DListLink::default(),
                val,
            }
        }
    }

    #[derive(Debug, Default)]
    struct DListAdapter(DefaultPointerOps<Box<DListItem>>);

    unsafe impl Adapter for DListAdapter {
        type PointerOps = DefaultPointerOps<Box<DListItem>>;

        type Link = DListLink;

        fn new() -> Self {
            Self::default()
        }

        fn pointer_ops(&self) -> &Self::PointerOps {
            &self.0
        }

        unsafe fn link2item(
            &self,
            link: *const Self::Link,
        ) -> *const <Self::PointerOps as PointerOps>::Item {
            crate::container_of!(link, DListItem, link)
        }

        unsafe fn item2link(
            &self,
            item: *const <Self::PointerOps as PointerOps>::Item,
        ) -> *const Self::Link {
            (item as *const u8).add(crate::offset_of!(DListItem, link)) as *const _
        }
    }

    #[test]
    fn test_dlist_simple() {
        let mut l = DList::<DListAdapter>::new();

        l.push_back(Box::new(DListItem::new(2)));
        l.push_front(Box::new(DListItem::new(1)));
        l.push_back(Box::new(DListItem::new(3)));

        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 2, 3]);
        assert_eq!(l.len(), 3);

        let mut iter = l.iter_mut();
        iter.next();
        iter.next();
        assert_eq!(iter.get().unwrap().val, 2);
        let i2 = iter.remove();
        assert_eq!(i2.unwrap().val, 2);
        assert_eq!(iter.get().unwrap().val, 3);
        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 3]);
        assert_eq!(l.len(), 2);

        let i3 = l.pop_back();
        assert_eq!(i3.unwrap().val, 3);
        let i1 = l.pop_front();
        assert_eq!(i1.unwrap().val, 1);
        assert!(l.pop_front().is_none());
        assert_eq!(l.len(), 0);
    }
}
