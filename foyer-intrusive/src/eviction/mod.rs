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

use crate::core::pointer::Pointer;
use std::fmt::Debug;

pub trait Config = Send + Sync + 'static + Debug + Clone;

pub trait EvictionPolicy: Send + Sync + 'static {
    type Pointer: Pointer;
    type Config: Config;

    fn new(config: Self::Config) -> Self;

    fn insert(&mut self, ptr: Self::Pointer);

    fn remove(&mut self, ptr: &Self::Pointer) -> Self::Pointer;

    fn access(&mut self, ptr: &Self::Pointer);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> impl Iterator<Item = &'_ Self::Pointer> + '_;
}

pub trait EvictionPolicyExt: EvictionPolicy {
    fn push(&mut self, ptr: Self::Pointer);

    fn pop(&mut self) -> Option<Self::Pointer>;

    fn peek(&self) -> Option<&Self::Pointer>;
}

impl<E: EvictionPolicy> EvictionPolicyExt for E
where
    <E as EvictionPolicy>::Pointer: Clone,
{
    fn push(&mut self, ptr: Self::Pointer) {
        self.insert(ptr)
    }

    fn pop(&mut self) -> Option<<Self as EvictionPolicy>::Pointer> {
        let ptr = {
            let mut iter = self.iter();
            let ptr = iter.next();
            ptr.cloned()
        };
        ptr.map(|ptr| self.remove(&ptr))
    }

    fn peek(&self) -> Option<&Self::Pointer> {
        self.iter().next()
    }
}

pub mod fifo;
pub mod lfu;
pub mod lru;
pub mod sfifo;
