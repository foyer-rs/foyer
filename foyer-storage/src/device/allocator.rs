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

use allocator_api2::{
    alloc::{AllocError, Allocator, Global},
    vec::Vec as VecA,
};
use foyer_common::bits;

pub struct WritableVecA<'a, T, A: Allocator>(pub &'a mut VecA<T, A>);

impl<'a, A: Allocator> std::io::Write for WritableVecA<'a, u8, A> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        let len = bufs.iter().map(|b| b.len()).sum();
        self.0.reserve(len);
        for buf in bufs {
            self.0.extend_from_slice(buf);
        }
        Ok(len)
    }

    // #[inline]
    // fn is_write_vectored(&self) -> bool {
    //     true
    // }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AlignedAllocator {
    align: usize,
}

impl AlignedAllocator {
    pub const fn new(align: usize) -> Self {
        assert!(align.is_power_of_two());
        Self { align }
    }
}

unsafe impl Allocator for AlignedAllocator {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, AllocError> {
        let layout =
            std::alloc::Layout::from_size_align(layout.size(), bits::align_up(self.align, layout.align())).unwrap();
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        let layout =
            std::alloc::Layout::from_size_align(layout.size(), bits::align_up(self.align, layout.align())).unwrap();
        Global.deallocate(ptr, layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer() {
        const ALIGN: usize = 512;
        let allocator = AlignedAllocator::new(ALIGN);

        let mut buf: VecA<u8, _> = VecA::with_capacity_in(ALIGN * 8, &allocator);
        bits::assert_aligned(ALIGN, buf.as_ptr() as _);

        buf.extend_from_slice(&[b'x'; ALIGN * 8]);
        bits::assert_aligned(ALIGN, buf.as_ptr() as _);
        assert_eq!(buf, [b'x'; ALIGN * 8]);

        buf.extend_from_slice(&[b'x'; ALIGN * 8]);
        bits::assert_aligned(ALIGN, buf.as_ptr() as _);
        assert_eq!(buf, [b'x'; ALIGN * 16])
    }
}
