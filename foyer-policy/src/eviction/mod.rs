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

use std::ptr::NonNull;

use crate::Item;

pub trait Policy: Send + Sync + 'static {
    type T: Item;
    type C: Config;
    type H: Handle<T = Self::T>;
    type E<'e>: Iterator<Item = &'e Self::T>;

    fn new(config: Self::C) -> Self;

    fn insert(&mut self, handle: NonNull<Self::H>) -> bool;

    fn remove(&mut self, handle: NonNull<Self::H>) -> bool;

    fn access(&mut self, handle: NonNull<Self::H>) -> bool;

    fn eviction_iter(&self) -> Self::E<'_>;
}

pub trait Config: Send + Sync + std::fmt::Debug + Clone + 'static {}

pub trait Handle: Send + Sync + 'static {
    type T: Item;

    fn new(index: Self::T) -> Self;

    fn item(&self) -> &Self::T;
}
