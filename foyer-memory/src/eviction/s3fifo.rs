use crate::CacheContext;
use crate::Key;
use crate::Value;
use crate::handle::BaseHandle;
use crate::handle::Handle;
use crate::eviction::Eviction;
use foyer_intrusive::collections::dlist::DlistLink;
use foyer_intrusive::collections::dlist::Dlist;
use std::ptr::NonNull;
use std::fmt::Debug;
use foyer_intrusive::intrusive_adapter;


#[derive(Debug, Clone)]
pub struct S3FifoContext;

impl From<CacheContext> for S3FifoContext {
    fn from(_: CacheContext) -> Self {
        Self
    }
}

impl From<S3FifoContext> for CacheContext {
    fn from(_: S3FifoContext) -> Self {
        CacheContext::Default
    }
}

pub struct S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    count: i32,
    base: BaseHandle<K, V, S3FifoContext>,
}

impl<K, V> Debug for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FifoHandle").finish()
    }
}

intrusive_adapter! { S3FifoHandleDlistAdapter<K, V> = NonNull<S3FifoHandle<K, V>>: S3FifoHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    pub fn inc (&mut self) {
        if self.count >= 3 {
            return;
        }
        self.count += 1;
    }

    pub fn dec (&mut self) {
        if self.count <= 0 {
            return;
        }
        self.count -= 1;
    }
}

impl<K, V> Handle for S3FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = S3FifoContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            count: 0,
            base: BaseHandle::new(),
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
        self.base.init(hash, key, value, charge, context);
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

#[derive(Debug, Clone)]
pub struct S3FifoConfig {}

pub struct S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    small_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,
    main_queue: Dlist<S3FifoHandleDlistAdapter<K, V>>,
    capacity: usize,
    small_used: usize,
    main_used: usize,
}

impl<K,V> S3Fifo<K,V>
where
    K: Key,
    V: Value,
{
    unsafe fn evict(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>> {
        if self.small_used > (0.1 * self.capacity as f64) as usize {
            match self.evict_small() {
                Some(ptr) => return Some(ptr),
                None => return self.evict_main(),
            }
        } else {
            return self.evict_main();
        }
    }

    unsafe fn evict_small(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>>{
        loop {
            let ptr = self.small_queue.pop_front();
            if ptr.is_none() {
                return None;
            }
            let mut ptr = ptr.unwrap();
            if ptr.as_mut().count > 1 {
                self.main_queue.push_back(ptr);
                self.small_used -= 1;
                self.main_used += 1;
            } else {
                self.small_used -= 1;
                return Some(ptr);
            }
        }
    }

    unsafe fn evict_main(&mut self) -> Option<NonNull<<S3Fifo<K, V> as Eviction>::Handle>>{
       loop {
            let ptr = self.main_queue.pop_front();
            if ptr.is_none() {
                return None;
            }
            let mut ptr = ptr.unwrap();
            if ptr.as_mut().count > 0 {
                self.main_queue.push_back(ptr);
                ptr.as_mut().dec();
            } else {
                // evict
                self.main_used -= 1;
                return Some(ptr);
            }
       }
    }
}

impl<K, V> Eviction for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = S3FifoHandle<K, V>;
    type Config = S3FifoConfig;

    unsafe fn new(capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self {
            small_queue: Dlist::new(),
            main_queue: Dlist::new(),
            capacity,
            small_used: 0,
            main_used: 0,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.small_queue.push_back(ptr);
        self.small_used += 1;
        ptr.as_mut().base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        self.evict()
    }

    unsafe fn reinsert(&mut self, _: NonNull<Self::Handle>) {
    }

    unsafe fn access(&mut self, ptr: NonNull<Self::Handle>) {
        let mut ptr = ptr;
        ptr.as_mut().inc();
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let p = self.small_queue.iter_mut_from_raw(ptr.as_mut().link.raw()).remove();
        if p.is_some() {
            self.small_used -= 1;
            assert_eq!(p.unwrap(), ptr);
            ptr.as_mut().base_mut().set_in_eviction(false);
            return;
        }
        let p = self.main_queue.iter_mut_from_raw(ptr.as_mut().link.raw()).remove();
        if p.is_some() {
            self.main_used -= 1;
            assert_eq!(p.unwrap(), ptr);
            ptr.as_mut().base_mut().set_in_eviction(false);
            return;
        }
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(mut ptr) = self.small_queue.pop_front() {
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }
        while let Some(mut ptr) = self.main_queue.pop_front() {
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }
        res
    }

    unsafe fn len(&self) -> usize {
        self.small_queue.len() + self.main_queue.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.small_queue.is_empty() && self.main_queue.is_empty()
    }
}

unsafe impl<K, V> Send for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for S3Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
