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

use crate::core::{
    adapter::{Adapter, KeyAdapter, Link},
    pointer::PointerOps,
};

use std::fmt::Debug;

pub trait Config = Send + Sync + 'static + Debug;

pub trait EvictionPolicy<A>: Send + Sync + 'static
where
    A: KeyAdapter<Link = Self::Link>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    type Link: Link;
    type Config: Config;
    type E<'e>: Iterator<Item = &'e <A::PointerOps as PointerOps>::Pointer>;

    fn new(config: Self::Config) -> Self;

    fn insert(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer);

    fn remove(
        &mut self,
        ptr: &<A::PointerOps as PointerOps>::Pointer,
    ) -> <A::PointerOps as PointerOps>::Pointer;

    fn access(&mut self, ptr: &<A::PointerOps as PointerOps>::Pointer);

    fn iter(&self) -> Self::E<'_>;

    fn push(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        self.insert(ptr)
    }

    fn pop(&mut self) -> Option<<A::PointerOps as PointerOps>::Pointer> {
        let ptr = {
            let mut iter = self.iter();
            let ptr = iter.next();
            ptr.cloned()
        };
        ptr.map(|ptr| self.remove(&ptr))
    }

    fn peek(&self) -> Option<&<A::PointerOps as PointerOps>::Pointer> {
        self.iter().next()
    }
}

pub mod lfu;
pub mod lru;
