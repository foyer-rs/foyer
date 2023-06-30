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

/// A mutable bytes slice without lifetime checks.
///
/// `slice` does NOT care about buffer allocator, because it never allocates or deallocatoes any buffer.
#[derive(Debug)]
pub struct SliceMut {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for SliceMut {}
unsafe impl Sync for SliceMut {}

impl SliceMut {
    /// # Safety
    ///
    /// `buf` MUST outlive `SliceMut`.
    pub unsafe fn new(buf: &mut [u8]) -> Self {
        let ptr = buf.as_mut_ptr();
        let len = buf.len();
        Self { ptr, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn seal(self) -> Slice {
        unsafe { Slice::new(self.as_ref()) }
    }
}

impl AsRef<[u8]> for SliceMut {
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsMut<[u8]> for SliceMut {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// An immutable bytes slice without lifetime checks.
///
/// `slice` does NOT care about buffer allocator, because it never allocates or deallocatoes any buffer.
#[derive(Debug)]
pub struct Slice {
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for Slice {}
unsafe impl Sync for Slice {}

impl Slice {
    /// # Safety
    ///
    /// `buf` MUST outlive `Slice`.
    pub unsafe fn new(buf: &[u8]) -> Self {
        let ptr = buf.as_ptr();
        let len = buf.len();
        Self { ptr, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}
