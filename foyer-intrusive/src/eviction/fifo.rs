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

//  Copyright (c) Meta Platforms, Inc. and affiliates.
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

use std::{mem::ManuallyDrop, ptr::NonNull};

use itertools::Itertools;

use crate::{
    collections::dlist::{DList, DListIter, DListLink},
    core::{
        adapter::{Adapter, Link, PriorityAdapter},
        pointer::PointerOps,
    },
    intrusive_adapter,
};

use super::EvictionPolicy;

#[derive(Debug, Clone)]
pub struct FifoConfig {
    /// `segment_ratios` is used to compute the ratio of each segment's size.
    ///
    /// The formula is as follows:
    ///
    /// `segment's size = total_segments * (segment's ratio / sum(ratio))`
    pub segment_ratios: Vec<usize>,
}

#[derive(Debug, Default)]
pub struct FifoLink {
    link_queue: DListLink,

    priority: usize,
}

impl FifoLink {
    fn raw(&self) -> NonNull<Self> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

impl Link for FifoLink {
    fn is_linked(&self) -> bool {
        self.link_queue.is_linked()
    }
}

intrusive_adapter! { FifoLinkAdapter = NonNull<FifoLink>: FifoLink { link_queue: DListLink } }

/// Segmented FIFO policy
///
/// It divides the fifo queue into N segments. Each segment holds
/// number of items proportional to its segment ratio. For example,
/// if we have 3 segments and the ratio of [2, 1, 1], the lowest
/// priority segment will hold 50% of the items whereas the other
/// two higher priority segments will hold 25% each.
///
/// On insertion, a priority is used as an Insertion Point. E.g. a pri-2
/// region will be inserted into the third highest priority segment. After
/// the insertion is completed, we will trigger rebalance, where this
/// region may be moved to below the insertion point, if the segment it
/// was originally inserted into had exceeded the size allowed by its ratio.
///
/// Our rebalancing scheme allows the lowest priority segment to grow beyond
/// its ratio allows for, since there is no lower segment to move into.
///
/// Also note that rebalancing is also triggered after an eviction.
///
/// The rate of inserting new regions look like the following:
/// Pri-2 ---
///          \
/// Pri-1 -------
///              \
/// Pri-0 ------------
/// When pri-2 exceeds its ratio, it effectively downgrades the oldest region in
/// pri-2 to pri-1, and that region is now pushed down at the combined rate of
/// (new pri-1 regions + new pri-2 regions), so effectively it gets evicted out
/// of the system faster once it is beyond the pri-2 segment ratio. Segment
/// ratio is put in place to prevent the lower segments getting so small a
/// portion of the flash device.
pub struct Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    // Note: All queue share the same dlist link.
    segments: Vec<DList<FifoLinkAdapter>>,

    config: FifoConfig,
    total_ratio: usize,

    len: usize,

    adapter: A,
}

impl<A> Drop for Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    fn drop(&mut self) {
        let mut to_remove = vec![];
        for ptr in self.iter() {
            to_remove.push(ptr.clone());
        }
        for ptr in to_remove {
            self.remove(&ptr);
        }
    }
}

impl<A> Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    pub fn new(config: FifoConfig) -> Self {
        let segments = (0..config.segment_ratios.len())
            .map(|_| DList::new())
            .collect_vec();
        let total_ratio = config.segment_ratios.iter().sum();

        Self {
            segments,

            config,
            total_ratio,

            len: 0,

            adapter: A::new(),
        }
    }

    fn insert(&mut self, ptr: <A::PointerOps as PointerOps>::Pointer) {
        unsafe {
            let item = self.adapter.pointer_ops().into_raw(ptr);
            let mut link = NonNull::new_unchecked(self.adapter.item2link(item) as *mut FifoLink);

            assert!(!link.as_ref().is_linked());

            let priority = *self.adapter.item2priority(item);
            link.as_mut().priority = priority.into();

            self.segments[priority.into()].push_back(link);

            self.rebalance();

            self.len += 1;
        }
    }

    fn remove(
        &mut self,
        ptr: &<A::PointerOps as PointerOps>::Pointer,
    ) -> <A::PointerOps as PointerOps>::Pointer {
        unsafe {
            let item = self.adapter.pointer_ops().as_ptr(ptr);
            let link = NonNull::new_unchecked(self.adapter.item2link(item) as *mut FifoLink);

            assert!(link.as_ref().is_linked());

            let priority = link.as_ref().priority;
            self.segments[priority]
                .iter_mut_from_raw(link.as_ref().link_queue.raw())
                .remove()
                .unwrap();

            self.rebalance();

            self.len -= 1;

            self.adapter.pointer_ops().from_raw(item)
        }
    }

    fn access(&mut self, _ptr: &<A::PointerOps as PointerOps>::Pointer) {}

    fn len(&self) -> usize {
        self.len
    }

    fn rebalance(&mut self) {
        unsafe {
            let total: usize = self.segments.iter().map(|queue| queue.len()).sum();

            // Rebalance from highest-pri segment to lowest-pri segment. This means the
            // lowest-pri segment can grow to far larger than its ratio suggests. This
            // is okay, as we only need higher-pri segments for items that are deemed
            // important.
            // e.g. {[a, b, c], [d], [e]} is a valid state for a SFIFO with 3 segments
            //      and a segment ratio of [1, 1, 1]
            for high in (1..self.segments.len()).rev() {
                let low = high - 1;
                let limit = (total as f64 * self.config.segment_ratios[high] as f64
                    / self.total_ratio as f64) as usize;
                while self.segments[high].len() > limit {
                    let mut link = self.segments[high].pop_front().unwrap();
                    link.as_mut().priority = low;
                    self.segments[low].push_back(link);
                }
            }
        }
    }

    fn iter(&self) -> FifoIter<A> {
        let mut iter_segments = self.segments.iter().map(|queue| queue.iter()).collect_vec();

        iter_segments.iter_mut().for_each(|iter| iter.front());

        FifoIter {
            lfu: self,
            iter_segments,
            segment: 0,

            ptr: ManuallyDrop::new(None),
        }
    }
}

pub struct FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    lfu: &'a Fifo<A>,
    iter_segments: Vec<DListIter<'a, FifoLinkAdapter>>,
    segment: usize,

    ptr: ManuallyDrop<Option<<<A as Adapter>::PointerOps as PointerOps>::Pointer>>,
}

impl<'a, A> FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    unsafe fn update_ptr(&mut self, link: NonNull<FifoLink>) {
        std::mem::forget(self.ptr.take());

        let item = self.lfu.adapter.link2item(link.as_ptr());
        let ptr = self.lfu.adapter.pointer_ops().from_raw(item);
        self.ptr = ManuallyDrop::new(Some(ptr));
    }

    unsafe fn ptr(&self) -> Option<&'a <<A as Adapter>::PointerOps as PointerOps>::Pointer> {
        if self.ptr.is_none() {
            return None;
        }
        let ptr = self.ptr.as_ref().unwrap();
        let raw = ptr as *const <<A as Adapter>::PointerOps as PointerOps>::Pointer;
        Some(&*raw)
    }
}

impl<'a, A> Iterator for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    type Item = &'a <A::PointerOps as PointerOps>::Pointer;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut link = None;
            while self.segment < self.iter_segments.len() {
                match self.iter_segments[self.segment].get().map(|l| l.raw()) {
                    Some(l) => {
                        self.iter_segments[self.segment].next();
                        link = Some(l);
                        break;
                    }
                    None => self.segment += 1,
                }
            }
            match link {
                None => None,
                Some(link) => {
                    self.update_ptr(link);
                    self.ptr()
                }
            }
        }
    }
}

// unsafe impl `Send + Sync` for structs with `NonNull` usage

unsafe impl<A> Send for Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
}

unsafe impl<A> Sync for Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
}

unsafe impl Send for FifoLink {}

unsafe impl Sync for FifoLink {}

unsafe impl<'a, A> Send for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
}

unsafe impl<'a, A> Sync for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
}

impl<A> EvictionPolicy for Fifo<A>
where
    A: Adapter<Link = FifoLink> + PriorityAdapter<Link = FifoLink>,
    <<A as Adapter>::PointerOps as PointerOps>::Pointer: Clone,
{
    type Adapter = A;

    type Config = FifoConfig;

    fn new(config: Self::Config) -> Self {
        Self::new(config)
    }

    fn insert(&mut self, ptr: <<A>::PointerOps as crate::core::pointer::PointerOps>::Pointer) {
        self.insert(ptr)
    }

    fn remove(
        &mut self,
        ptr: &<<A>::PointerOps as crate::core::pointer::PointerOps>::Pointer,
    ) -> <<A>::PointerOps as crate::core::pointer::PointerOps>::Pointer {
        self.remove(ptr)
    }

    fn access(&mut self, ptr: &<<A>::PointerOps as crate::core::pointer::PointerOps>::Pointer) {
        self.access(ptr)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn iter(&self) -> impl Iterator<Item = &'_ <A::PointerOps as PointerOps>::Pointer> {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::priority_adapter;

    use super::*;

    #[derive(Debug)]
    struct FifoItem {
        link: FifoLink,
        key: u64,
        priority: usize,
    }

    impl FifoItem {
        fn new(key: u64, priority: usize) -> Self {
            Self {
                link: FifoLink::default(),
                key,
                priority,
            }
        }
    }

    intrusive_adapter! { FifoItemAdapter = Arc<FifoItem>: FifoItem { link: FifoLink } }
    priority_adapter! { FifoItemAdapter = FifoItem { priority: usize } }

    #[test]
    fn test_fifo_simple() {
        let config = FifoConfig {
            segment_ratios: vec![6, 3, 1],
        };
        let mut fifo = Fifo::<FifoItemAdapter>::new(config);

        let mut items = vec![];

        // see comments in `rebalance`
        // inner: [[0, 1, 2, 3, 4, 5, 6], [7, 8], [9]]
        for key in 0..10 {
            let item = Arc::new(FifoItem::new(key, 2));
            fifo.push(item.clone());
            items.push(item);
        }
        let v = fifo.iter().map(|item| item.key).collect_vec();
        assert_eq!(v, (0..10).collect_vec());
        let lens = fifo.segments.iter().map(|queue| queue.len()).collect_vec();
        assert_eq!(lens, vec![7, 2, 1]);

        // inner: [[0, 1, 2, 3, 4, 5, 6], [7, 8, 10], [9]]
        let item = Arc::new(FifoItem::new(10, 1));
        fifo.push(item.clone());
        items.push(item);
        let v = fifo.iter().map(|item| item.key).collect_vec();
        assert_eq!(v, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 9]);

        // inner: [[0, 1, 2, 3, 4, 5, 6], [7, 10], [9]]
        fifo.remove(&items[8]);
        let v = fifo.iter().map(|item| item.key).collect_vec();
        assert_eq!(v, vec![0, 1, 2, 3, 4, 5, 6, 7, 10, 9]);

        drop(fifo);

        for item in items {
            assert_eq!(Arc::strong_count(&item), 1);
        }
    }
}
