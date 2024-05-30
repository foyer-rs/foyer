//  Copyright 2024 Foyer Project Authors
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

#[cfg(feature = "sanity")]
use crate::handle::Handle;

use super::Eviction;

pub struct SanityEviction<E>
where
    E: Eviction,
{
    eviction: E,
}

#[cfg(feature = "sanity")]
impl<E> Eviction for SanityEviction<E>
where
    E: Eviction,
{
    type Handle = E::Handle;
    type Config = E::Config;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self {
            eviction: E::new(capacity, config),
        }
    }

    unsafe fn push(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        assert!(!ptr.as_ref().base().is_in_eviction());
        self.eviction.push(ptr);
        assert!(ptr.as_ref().base().is_in_eviction());
    }

    unsafe fn pop(&mut self) -> Option<std::ptr::NonNull<Self::Handle>> {
        let res = self.eviction.pop();
        if let Some(ptr) = res {
            assert!(!ptr.as_ref().base().is_in_eviction());
        }
        res
    }

    unsafe fn acquire(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.acquire(ptr)
    }

    unsafe fn release(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.release(ptr)
    }

    unsafe fn remove(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        assert!(ptr.as_ref().base().is_in_eviction());
        self.eviction.remove(ptr);
        assert!(!ptr.as_ref().base().is_in_eviction());
    }

    unsafe fn clear(&mut self) -> Vec<std::ptr::NonNull<Self::Handle>> {
        self.eviction.clear()
    }

    fn len(&self) -> usize {
        self.eviction.len()
    }

    fn is_empty(&self) -> bool {
        self.eviction.is_empty()
    }
}

#[cfg(not(feature = "sanity"))]
impl<E> Eviction for SanityEviction<E>
where
    E: Eviction,
{
    type Handle = E::Handle;
    type Config = E::Config;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self {
            eviction: E::new(capacity, config),
        }
    }

    unsafe fn push(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.push(ptr)
    }

    unsafe fn pop(&mut self) -> Option<std::ptr::NonNull<Self::Handle>> {
        self.eviction.pop()
    }

    unsafe fn acquire(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.acquire(ptr)
    }

    unsafe fn release(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.release(ptr)
    }

    unsafe fn remove(&mut self, ptr: std::ptr::NonNull<Self::Handle>) {
        self.eviction.remove(ptr)
    }

    unsafe fn clear(&mut self) -> Vec<std::ptr::NonNull<Self::Handle>> {
        self.eviction.clear()
    }

    fn len(&self) -> usize {
        self.eviction.len()
    }

    fn is_empty(&self) -> bool {
        self.eviction.is_empty()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{eviction::test_utils::TestEviction, handle::Handle};

    impl<T, E> TestEviction for SanityEviction<E>
    where
        T: Send + Sync + 'static + Clone,
        E: TestEviction,
        E::Handle: Handle<Data = T>,
    {
        fn dump(&self) -> Vec<T> {
            self.eviction.dump()
        }
    }
}
