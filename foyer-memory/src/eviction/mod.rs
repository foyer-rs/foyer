//  Copyright 2024 MrCroxx
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

use crate::handle::Handle;

/// The lifetime of `handle: Self::H` is managed by [`Indexer`].
///
/// Each `handle`'s lifetime in [`Indexer`] must outlive the raw pointer in [`Eviction`].
pub trait Eviction: Send + Sync + 'static {
    type H: Handle;
    type C: Clone;

    fn new(config: Self::C) -> Self;
    fn push(&mut self, ptr: NonNull<Self::H>);
    fn pop(&mut self) -> Option<NonNull<Self::H>>;
    fn peek(&self) -> Option<NonNull<Self::H>>;
    fn access(&mut self, ptr: NonNull<Self::H>);
    fn remove(&mut self, ptr: NonNull<Self::H>);
    fn clear(&mut self) -> Vec<NonNull<Self::H>>;
    fn is_empty(&self) -> bool;
}

pub mod fifo;
