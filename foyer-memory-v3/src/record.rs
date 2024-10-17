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
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct RecordFlags: u64 {
        const IN_INDEXER = 0b00000001;
        const IN_EVICTION = 0b00000010;
        const DEPOSIT= 0b00000100;
    }
}

pub struct RecordData<K, V, H, S> {
    pub key: K,
    pub value: V,
    pub hint: H,
    pub state: S,
    pub hash: u64,
    pub weight: usize,
}

/// [`Record`] is a continuous piece of heap allocated memory, stores per entry data.
///
/// The lifetime of [`Record`] is managed by foyer. It can only be accessed by [`RecordResolver`] with [`RecordToken`].
pub struct Record<K, V, H, S> {
    data: Option<RecordData<K, V, H, S>>,
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

    pub fn set(&mut self, data: RecordData<K, V, H, S>) {
        let old = self.data.replace(data);
        assert!(old.is_none());
    }

    pub fn take(&mut self) -> RecordData<K, V, H, S> {
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
        self.set_flags(RecordFlags::IN_INDEXER, val, order);
    }

    pub fn is_in_indexer(&self, order: Ordering) -> bool {
        self.get_flags(RecordFlags::IN_INDEXER, order)
    }

    pub fn set_in_eviction(&self, val: bool, order: Ordering) {
        self.set_flags(RecordFlags::IN_EVICTION, val, order);
    }

    pub fn is_in_eviction(&self, order: Ordering) -> bool {
        self.get_flags(RecordFlags::IN_EVICTION, order)
    }

    pub fn set_deposit(&self, val: bool, order: Ordering) {
        self.set_flags(RecordFlags::DEPOSIT, val, order);
    }

    pub fn is_deposit(&self, order: Ordering) -> bool {
        self.get_flags(RecordFlags::DEPOSIT, order)
    }

    pub fn set_flags(&self, flags: RecordFlags, val: bool, order: Ordering) {
        match val {
            true => self.flags.fetch_or(flags.bits(), order),
            false => self.flags.fetch_and(!flags.bits(), order),
        };
    }

    pub fn get_flags(&self, flags: RecordFlags, order: Ordering) -> bool {
        self.flags.fetch_and(flags.bits(), order) != 0
    }
}

pub struct RecordToken<K, V, H, S> {
    ptr: NonNull<Record<K, V, H, S>>,
}

unsafe impl<K, V, H, S> Send for RecordToken<K, V, H, S> {}
unsafe impl<K, V, H, S> Sync for RecordToken<K, V, H, S> {}

impl<K, V, H, S> Clone for RecordToken<K, V, H, S> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

/// [`RecordResolver`] resolves [`RecordToken`] to immutable or mutable reference of [`Record`].
///
/// [`RecordResolver`] doesn't do anything buf dereference the pointer of the [`Record`].
///
/// The advantage of using [`RecordResolver`] over using [`std::ops::Deref`] or [`std::ops::DerefMut`] directly is that
/// [`RecordResolver`] can bind the mutability of the [`Record`] with the owner of [`RecordResolver`], instead of the
/// [`RecordToken`].
///
/// It is useful for providing a safe API for the user when implementing a customized eviction algorithm with the
/// technique of intrusive data structure while letting foyer manage the lifetime of a [`Record`].
pub struct RecordResolver<K, V, H, S>(PhantomData<(K, V, H, S)>);

impl<K, V, H, S> RecordResolver<K, V, H, S> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }

    pub fn resolve(&self, token: &RecordToken<K, V, H, S>) -> &Record<K, V, H, S> {
        unsafe { token.ptr.as_ref() }
    }

    pub fn resolve_mut(&mut self, token: &mut RecordToken<K, V, H, S>) -> &mut Record<K, V, H, S> {
        unsafe { token.ptr.as_mut() }
    }
}

#[derive(Debug, Default)]
pub struct Link {
    perv: Option<NonNull<Link>>,
    next: Option<NonNull<Link>>,
}

unsafe impl Send for Link {}
unsafe impl Sync for Link {}

pub struct DefaultRecordTokenList;

pub trait RecordTokenListState<ID = DefaultRecordTokenList> {
    fn link(&mut self) -> &mut Link;
}

pub struct RecordTokenList<ID, K, V, H, S> {
    head: Option<RecordToken<K, V, H, S>>,
    tail: Option<RecordToken<K, V, H, S>>,

    len: usize,

    resolver: RecordResolver<K, V, H, S>,

    _marker: PhantomData<ID>,
}

impl<ID, K, V, H, S> RecordTokenList<ID, K, V, H, S>
where
    S: RecordTokenListState<ID>,
{
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,

            resolver: RecordResolver::new(),

            _marker: PhantomData,
        }
    }

    // pub fn push_back(&mut self, token: RecordToken<K, V, H, S>) {}
}

pub struct RecordTokenListIter<'a, ID, K, V, H, S> {
    token: Option<RecordToken<K, V, H, S>>,
    list: &'a RecordTokenList<ID, K, V, H, S>,
}

pub struct RecordTokenListIterMut<'a, ID, K, V, H, S> {
    token: Option<RecordToken<K, V, H, S>>,
    list: &'a mut RecordTokenList<ID, K, V, H, S>,
}

impl<'a, ID, K, V, H, S> RecordTokenListIterMut<'a, ID, K, V, H, S>
where
    S: RecordTokenListState<ID>,
{
    pub fn is_valid(&self) -> bool {
        self.token.is_some()
    }

    pub fn record(&self) -> Option<&Record<K, V, H, S>> {
        self.token.as_ref().map(|token| self.list.resolver.resolve(token))
    }

    pub fn record_mut(&mut self) -> Option<&mut Record<K, V, H, S>> {
        self.token.as_mut().map(|token| self.list.resolver.resolve_mut(token))
    }

    pub fn next(&mut self) {
        self.token = match self.token {
            Some(token) => self.list.resolver.resolve(&token).state().link().perv,
            None => self.list.head.clone(),
        }
    }
}
