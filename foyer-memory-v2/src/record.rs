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

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u64 {
        const IN_INDEXER = 0b00000001;
        const IN_EVICTION = 0b00000010;
        const DEPOSIT= 0b00000100;
    }
}

pub struct Data<K, V, H, S> {
    pub key: K,
    pub value: V,
    pub hint: H,
    pub state: S,
    pub hash: u64,
    pub weight: usize,
}

pub struct Record<K, V, H, S> {
    data: Option<Data<K, V, H, S>>,
    refs: AtomicUsize,
    flags: AtomicU64,
}

impl<K, V, H, S> Record<K, V, H, S> {
    pub fn new() -> Self {
        Self {
            data: None,
            refs: AtomicUsize::new(0),
            flags: AtomicU64::new(0),
        }
    }

    pub fn set(&mut self, data: Data<K, V, H, S>) {
        let old = self.data.replace(data);
        assert!(old.is_none());
    }

    pub fn take(&mut self) -> Data<K, V, H, S> {
        self.data.take().unwrap()
    }

    pub fn key(&self) -> &K {
        &self.data.as_ref().unwrap().key
    }

    pub fn value(&self) -> &V {
        &self.data.as_ref().unwrap().value
    }

    pub fn hint(&self) -> &H {
        &self.data.as_ref().unwrap().hint
    }

    pub fn state(&self) -> &S {
        &self.data.as_ref().unwrap().state
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.data.as_mut().unwrap().state
    }

    pub fn hash(&self) -> u64 {
        self.data.as_ref().unwrap().hash
    }

    pub fn weight(&self) -> usize {
        self.data.as_ref().unwrap().weight
    }

    pub fn refs(&self) -> &AtomicUsize {
        &self.refs
    }

    pub fn flags(&self) -> &AtomicU64 {
        &self.flags
    }

    pub fn set_in_indexer(&self, val: bool, order: Ordering) {
        self.set_flags(Flags::IN_INDEXER, val, order);
    }

    pub fn is_in_indexer(&self, order: Ordering) -> bool {
        self.get_flags(Flags::IN_INDEXER, order)
    }

    pub fn set_in_eviction(&self, val: bool, order: Ordering) {
        self.set_flags(Flags::IN_EVICTION, val, order);
    }

    pub fn is_in_eviction(&self, order: Ordering) -> bool {
        self.get_flags(Flags::IN_EVICTION, order)
    }

    pub fn set_deposit(&self, val: bool, order: Ordering) {
        self.set_flags(Flags::DEPOSIT, val, order);
    }

    pub fn is_deposit(&self, order: Ordering) -> bool {
        self.get_flags(Flags::DEPOSIT, order)
    }

    pub fn set_flags(&self, flags: Flags, val: bool, order: Ordering) {
        match val {
            true => self.flags.fetch_or(flags.bits(), order),
            false => self.flags.fetch_and(!flags.bits(), order),
        };
    }

    pub fn get_flags(&self, flags: Flags, order: Ordering) -> bool {
        self.flags.fetch_and(flags.bits(), order) != 0
    }
}

/// A reference to the heap allocated record.
pub struct RecordRef<K, V, H, S> {
    ptr: NonNull<Record<K, V, H, S>>,
}

unsafe impl<K, V, H, S> Send for RecordRef<K, V, H, S> {}
unsafe impl<K, V, H, S> Sync for RecordRef<K, V, H, S> {}

impl<K, V, H, S> PartialEq for RecordRef<K, V, H, S> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<K, V, H, S> Eq for RecordRef<K, V, H, S> {}

impl<K, V, H, S> Deref for RecordRef<K, V, H, S> {
    type Target = Record<K, V, H, S>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<K, V, H, S> DerefMut for RecordRef<K, V, H, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<K, V, H, S> RecordRef<K, V, H, S> {
    pub fn new(ptr: NonNull<Record<K, V, H, S>>) -> Self {
        Self { ptr }
    }
}

pub struct RecordToken<K, V, H, S> {
    ptr: NonNull<Record<K, V, H, S>>,
}

impl<K, V, H, S> PartialEq for RecordToken<K, V, H, S> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<K, V, H, S> Eq for RecordToken<K, V, H, S> {}

impl<K, V, H, S> Clone for RecordToken<K, V, H, S> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

pub struct RecordResolver<K, V, H, S>(PhantomData<(K, V, H, S)>);

impl<K, V, H, S> RecordResolver<K, V, H, S> {
    pub fn resolve(&self, token: RecordToken<K, V, H, S>) -> &Record<K, V, H, S> {
        unsafe { token.ptr.as_ref() }
    }

    pub fn resolve_mut(&mut self, mut token: RecordToken<K, V, H, S>) -> &mut Record<K, V, H, S> {
        unsafe { token.ptr.as_mut() }
    }
}

pub struct Link {
    perv: Option<NonNull<Link>>,
    next: Option<NonNull<Link>>,
}

unsafe impl Send for Link {}
unsafe impl Sync for Link {}

pub struct DefaultRecordTokenList;

pub trait Item<ID = DefaultRecordTokenList> {
    fn link(&mut self) -> &mut Link;
}

pub struct RecordTokenList<ID, T> {
    head: Option<NonNull<Link>>,
    tail: Option<NonNull<Link>>,

    len: usize,

    _marker: PhantomData<(ID, T)>,
}

unsafe impl<ID, T> Send for RecordTokenList<ID, T> {}
unsafe impl<ID, T> Sync for RecordTokenList<ID, T> {}

impl<ID, T> Default for RecordTokenList<ID, T>
where
    T: Item<ID>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ID, T> RecordTokenList<ID, T>
where
    T: Item<ID>,
{
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
            _marker: PhantomData,
        }
    }

    pub fn push_back(&mut self, item: &mut Box<T>) {}
}
