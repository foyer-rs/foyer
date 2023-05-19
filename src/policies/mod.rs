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

//! A collection of cache eviction policies.
//!
//! Cache eviction policies only cares about the order of cached entries.
//! They don't store the real cache entries or resource usage.

pub mod lru;
pub mod tinylfu;

use std::hash::Hash;

pub trait Index: Clone + Hash + Send + Sync + 'static {}

pub trait Policy: Send + Sync + 'static {}

#[derive(PartialEq, Eq, Debug)]
pub enum AccessMode {
    Read,
    Write,
}

pub enum HandleInner<I: Index> {
    LruHandle(lru::Handle<I>),
}

#[allow(unused)]
pub struct Handle<I: Index> {
    inner: HandleInner<I>,
}

#[cfg(test)]
impl Index for u64 {}
