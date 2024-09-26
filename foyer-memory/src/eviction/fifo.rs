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

use std::{fmt::Debug, ptr::NonNull};

use foyer_intrusive::{
    dlist::{Dlist, DlistLink},
    intrusive_adapter,
};
use serde::{Deserialize, Serialize};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext,
};

#[derive(Debug, Clone)]
pub struct FifoContext(CacheContext);

impl From<CacheContext> for FifoContext {
    fn from(context: CacheContext) -> Self {
        Self(context)
    }
}

impl From<FifoContext> for CacheContext {
    fn from(context: FifoContext) -> Self {
        context.0
    }
}

pub struct FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    link: DlistLink,
    base: BaseHandle<T, FifoContext>,
}

impl<T> Debug for FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FifoHandle").finish()
    }
}

intrusive_adapter! { FifoHandleDlistAdapter<T> = FifoHandle<T> { link: DlistLink } where T: Send + Sync + 'static }

impl<T> Default for FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            link: DlistLink::default(),
            base: BaseHandle::new(),
        }
    }
}

impl<T> Handle for FifoHandle<T>
where
    T: Send + Sync + 'static,
{
    type Data = T;
    type Context = FifoContext;

    fn base(&self) -> &BaseHandle<Self::Data, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Data, Self::Context> {
        &mut self.base
    }
}

/// Fifo eviction algorithm config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FifoConfig {}

pub struct Fifo<T>
where
    T: Send + Sync + 'static,
{
    queue: Dlist<FifoHandleDlistAdapter<T>>,
}

impl<T> Eviction for Fifo<T>
where
    T: Send + Sync + 'static,
{
    type Handle = FifoHandle<T>;
    type Config = FifoConfig;

    unsafe fn new(_capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self { queue: Dlist::new() }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.queue.push_back(ptr);
        ptr.as_mut().base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        self.queue.pop_front().map(|mut ptr| {
            ptr.as_mut().base_mut().set_in_eviction(false);
            ptr
        })
    }

    unsafe fn release(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn acquire(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let p = self.queue.iter_mut_from_raw(ptr.as_mut().link.raw()).remove().unwrap();
        assert_eq!(p, ptr);
        ptr.as_mut().base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(mut ptr) = self.queue.pop_front() {
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }
        res
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<T> Send for Fifo<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for Fifo<T> where T: Send + Sync + 'static {}

#[cfg(test)]
pub mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::{eviction::test_utils::TestEviction, handle::HandleExt};

    impl<T> TestEviction for Fifo<T>
    where
        T: Send + Sync + 'static + Clone,
    {
        fn dump(&self) -> Vec<T> {
            self.queue
                .iter()
                .map(|handle| handle.base().data_unwrap_unchecked().clone())
                .collect_vec()
        }
    }

    type TestFifoHandle = FifoHandle<u64>;
    type TestFifo = Fifo<u64>;

    unsafe fn new_test_fifo_handle_ptr(data: u64) -> NonNull<TestFifoHandle> {
        let mut handle = Box::<TestFifoHandle>::default();
        handle.init(0, data, 1, FifoContext(CacheContext::Default));
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_fifo_handle_ptr(ptr: NonNull<TestFifoHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_fifo() {
        unsafe {
            let ptrs = (0..8).map(|i| new_test_fifo_handle_ptr(i)).collect_vec();

            let mut fifo = TestFifo::new(100, &FifoConfig {});

            // 0, 1, 2, 3
            fifo.push(ptrs[0]);
            fifo.push(ptrs[1]);
            fifo.push(ptrs[2]);
            fifo.push(ptrs[3]);

            // 2, 3
            let p0 = fifo.pop().unwrap();
            let p1 = fifo.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(ptrs[1], p1);

            // 2, 3, 4, 5, 6
            fifo.push(ptrs[4]);
            fifo.push(ptrs[5]);
            fifo.push(ptrs[6]);

            // 2, 6
            fifo.remove(ptrs[3]);
            fifo.remove(ptrs[4]);
            fifo.remove(ptrs[5]);

            assert_eq!(fifo.clear(), vec![ptrs[2], ptrs[6]]);

            for ptr in ptrs {
                del_test_fifo_handle_ptr(ptr);
            }
        }
    }
}
