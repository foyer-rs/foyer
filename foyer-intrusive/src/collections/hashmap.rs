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

use std::{marker::PhantomData, ptr::NonNull};

use foyer_common::code::{Key, Value};
use std::hash::Hasher;
use twox_hash::XxHash64;

use crate::{
    core::{
        adapter::{KeyAdapter, Link},
        pointer::PointerOps,
    },
    intrusive_adapter,
};

use super::dlist::{DList, DListIter, DListIterMut, DListLink};

#[derive(Debug, Default)]
pub struct HashMapLink {
    dlist_link: DListLink,
}

intrusive_adapter! { HashMapLinkAdapter = NonNull<HashMapLink>: HashMapLink { dlist_link: DListLink } }

impl HashMapLink {
    pub fn raw(&self) -> NonNull<HashMapLink> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

unsafe impl Send for HashMapLink {}
unsafe impl Sync for HashMapLink {}

impl Link for HashMapLink {
    fn is_linked(&self) -> bool {
        self.dlist_link.is_linked()
    }
}

pub struct HashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = HashMapLink>,
{
    slots: Vec<DList<HashMapLinkAdapter>>,

    len: usize,

    adapter: A,

    _marker: PhantomData<V>,
}

impl<K, V, A> Drop for HashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = HashMapLink>,
{
    fn drop(&mut self) {
        unsafe {
            for slot in self.slots.iter_mut() {
                let mut iter = slot.iter_mut();
                iter.front();
                while iter.is_valid() {
                    let link = iter.remove().unwrap();
                    let item = self.adapter.link2item(link.as_ptr());
                    let _ = A::PointerOps::from_raw(item);
                }
            }
        }
    }
}

impl<K, V, A> HashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = HashMapLink>,
{
    pub fn new(bits: usize) -> Self {
        let mut slots = Vec::with_capacity(1 << bits);
        for _ in 0..(1 << bits) {
            slots.push(DList::new());
        }
        Self {
            slots,
            len: 0,
            adapter: A::new(),
            _marker: PhantomData,
        }
    }

    pub fn insert(
        &mut self,
        ptr: A::PointerOps,
    ) -> Option<A::PointerOps> {
        unsafe {
            let item_new = A::PointerOps::into_raw(ptr);
            let link_new = NonNull::new_unchecked(self.adapter.item2link(item_new) as *mut A::Link);

            let key_new = &*self.adapter.item2key(item_new);
            let hash = self.hash_key(key_new);
            let slot = (self.slots.len() - 1) & hash as usize;

            let res = self.remove_inner(key_new, slot);
            if res.is_some() {
                self.len -= 1;
            }

            self.slots[slot].push_front(link_new);

            self.len += 1;

            res
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<A::PointerOps> {
        unsafe {
            let hash = self.hash_key(key);
            let slot = (self.slots.len() - 1) & hash as usize;

            let res = self.remove_inner(key, slot);
            if res.is_some() {
                self.len -= 1;
            }
            res
        }
    }

    pub fn lookup(&self, key: &K) -> Option<&<A::PointerOps as PointerOps>::Item> {
        unsafe {
            let hash = self.hash_key(key);
            let slot = (self.slots.len() - 1) & hash as usize;

            match self.lookup_inner(key, slot) {
                Some(iter) => {
                    let link = iter.get().unwrap() as *const _;
                    let item = &*self.adapter.link2item(link);
                    Some(item)
                }
                None => None,
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> HashMapIter<'_, K, V, A> {
        HashMapIter::new(self)
    }

    /// # Safety
    ///
    /// `link` MUST be in this [`HashMap`].
    pub unsafe fn remove_in_place(
        &mut self,
        link: NonNull<HashMapLink>,
    ) -> A::PointerOps {
        assert!(link.as_ref().is_linked());
        let item = self.adapter.link2item(link.as_ptr());
        let key = &*self.adapter.item2key(item);
        let hash = self.hash_key(key);
        let slot = (self.slots.len() - 1) & hash as usize;
        self.slots[slot]
            .iter_mut_from_raw(link.as_ref().dlist_link.raw())
            .remove();
        self.len -= 1;
        A::PointerOps::from_raw(item)
    }

    /// # Safety
    ///
    /// there must be at most one matches in the slot
    unsafe fn lookup_inner_mut(
        &mut self,
        key: &K,
        slot: usize,
    ) -> Option<DListIterMut<'_, HashMapLinkAdapter>> {
        let mut iter = self.slots[slot].iter_mut();
        iter.front();
        while iter.is_valid() {
            let item = self.adapter.link2item(iter.get().unwrap().raw().as_ptr());
            let ikey = &*self.adapter.item2key(item);
            if ikey == key {
                return Some(iter);
            }
            iter.next();
        }
        None
    }

    /// # Safety
    ///
    /// there must be at most one matches in the slot
    unsafe fn lookup_inner(
        &self,
        key: &K,
        slot: usize,
    ) -> Option<DListIter<'_, HashMapLinkAdapter>> {
        let mut iter = self.slots[slot].iter();
        iter.front();
        while iter.is_valid() {
            let item = self.adapter.link2item(iter.get().unwrap().raw().as_ptr());
            let ikey = &*self.adapter.item2key(item);
            if ikey == key {
                return Some(iter);
            }
            iter.next();
        }
        None
    }

    /// # Safety
    ///
    /// there must be at most one matches in the slot
    unsafe fn remove_inner(
        &mut self,
        key: &K,
        slot: usize,
    ) -> Option<A::PointerOps> {
        match self.lookup_inner_mut(key, slot) {
            Some(mut iter) => {
                let link = iter.remove().unwrap();
                let item = self.adapter.link2item(link.as_ptr());
                let ptr = A::PointerOps::from_raw(item);
                Some(ptr)
            }
            None => None,
        }
    }

    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

pub struct HashMapIter<'a, K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = HashMapLink>,
{
    slot: usize,
    iters: Vec<DListIter<'a, HashMapLinkAdapter>>,

    adapter: A,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V, A> HashMapIter<'a, K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = HashMapLink>,
{
    pub fn new(hashmap: &'a HashMap<K, V, A>) -> Self {
        let mut iters = Vec::with_capacity(hashmap.slots.len());
        for slot in hashmap.slots.iter() {
            let iter = slot.iter();
            iters.push(iter);
        }
        Self {
            slot: 0,
            iters,

            adapter: A::new(),
            _marker: PhantomData,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.iters[self.slot].is_valid()
    }

    pub fn get(&self) -> Option<&<A::PointerOps as PointerOps>::Item> {
        self.iters[self.slot]
            .get()
            .map(|link| unsafe { &*(self.adapter.link2item(link.raw().as_ptr()) as *const _) })
    }

    pub fn get_mut(&mut self) -> Option<&mut <A::PointerOps as PointerOps>::Item> {
        self.iters[self.slot]
            .get()
            .map(|link| unsafe { &mut *(self.adapter.link2item(link.raw().as_ptr()) as *mut _) })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        if self.iters[self.slot].is_valid() {
            self.iters[self.slot].next();
            if !self.iters[self.slot].is_valid() && self.slot + 1 < self.iters.len() {
                self.slot += 1;
                self.iters[self.slot].front();
                debug_assert!(self.is_valid());
            }
        } else {
            self.front();
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        if self.iters[self.slot].is_valid() {
            self.iters[self.slot].prev();
            if !self.iters[self.slot].is_valid() && self.slot > 0 {
                self.slot -= 1;
                self.iters[self.slot].back();
                debug_assert!(self.is_valid());
            }
        } else {
            self.back();
        }
    }

    /// Move to front.
    pub fn front(&mut self) {
        self.slot = 0;
        self.iters[self.slot].front();
    }

    /// Move to back.
    pub fn back(&mut self) {
        self.slot = self.iters.len() - 1;
        self.iters[self.slot].back();
    }
}

// TODO(MrCroxx): Need more tests.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{intrusive_adapter, key_adapter};

    use super::*;

    #[derive(Debug)]
    struct HashMapItem {
        link: HashMapLink,

        key: u64,
        value: u64,
    }

    intrusive_adapter! { HashMapItemAdapter = Arc<HashMapItem>: HashMapItem { link: HashMapLink } }
    key_adapter! { HashMapItemAdapter = HashMapItem { key: u64 } }

    #[test]
    fn test_hashmap_simple() {
        let mut map = HashMap::<u64, u64, HashMapItemAdapter>::new(6);
        let items = (0..128)
            .map(|i| HashMapItem {
                link: HashMapLink::default(),
                key: i,
                value: i,
            })
            .map(Arc::new)
            .collect_vec();

        for item in items.iter() {
            map.insert(item.clone());
        }

        for i in 0..128 {
            let item = map.lookup(&i).unwrap();
            assert_eq!(item.key, i);
            assert_eq!(item.value, i);
        }

        for i in 0..64 {
            assert!(map.remove(&i).is_some());
        }

        for i in 0..64 {
            assert!(map.lookup(&i).is_none());
        }
        for i in 64..128 {
            let item = map.lookup(&i).unwrap();
            assert_eq!(item.key, i);
            assert_eq!(item.value, i);
        }

        for item in items.iter() {
            map.insert(item.clone());
        }
        for i in 0..128 {
            let item = map.lookup(&i).unwrap();
            assert_eq!(item.key, i);
            assert_eq!(item.value, i);
        }

        unsafe { map.remove_in_place(items[0].link.raw()) };
        assert!(map.lookup(&0).is_none());

        drop(map);

        for item in items {
            assert_eq!(Arc::strong_count(&item), 1);
        }
    }
}
