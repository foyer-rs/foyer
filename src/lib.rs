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

#![feature(trait_alias)]
#![feature(pattern)]

pub use policies::Policy;

pub mod collections;
pub mod container;
pub mod policies;
pub mod store;

pub trait Index:
    PartialOrd + Ord + PartialEq + Eq + Clone + std::hash::Hash + Send + Sync + 'static
{
    fn size() -> usize;

    fn write(&self, buf: &mut [u8]);

    fn read(buf: &[u8]) -> Self;
}
pub trait Data = Send + Sync + 'static + Into<Vec<u8>> + From<Vec<u8>>;

#[cfg(test)]

pub mod tests {
    use bytes::{Buf, BufMut};

    use super::*;

    pub fn is_send_sync_static<T: Send + Sync + 'static>() {}

    impl Index for u64 {
        fn size() -> usize {
            8
        }

        fn write(&self, mut buf: &mut [u8]) {
            buf.put_u64(*self)
        }

        fn read(mut buf: &[u8]) -> Self {
            buf.get_u64()
        }
    }
}
