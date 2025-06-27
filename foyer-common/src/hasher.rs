// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::{BuildHasher, Hasher};

/// A hasher return u64 mod result.
#[derive(Debug, Default)]
pub struct ModHasher {
    state: u64,
}

impl Hasher for ModHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state = (self.state << 8) + *byte as u64;
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.write(&[i])
    }

    fn write_u16(&mut self, i: u16) {
        self.write(&i.to_be_bytes())
    }

    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_be_bytes())
    }

    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_be_bytes())
    }

    fn write_u128(&mut self, i: u128) {
        self.write(&i.to_be_bytes())
    }

    fn write_usize(&mut self, i: usize) {
        self.write(&i.to_be_bytes())
    }

    fn write_i8(&mut self, i: i8) {
        self.write_u8(i as u8)
    }

    fn write_i16(&mut self, i: i16) {
        self.write_u16(i as u16)
    }

    fn write_i32(&mut self, i: i32) {
        self.write_u32(i as u32)
    }

    fn write_i64(&mut self, i: i64) {
        self.write_u64(i as u64)
    }

    fn write_i128(&mut self, i: i128) {
        self.write_u128(i as u128)
    }

    fn write_isize(&mut self, i: isize) {
        self.write_usize(i as usize)
    }
}

impl BuildHasher for ModHasher {
    type Hasher = Self;

    fn build_hasher(&self) -> Self::Hasher {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mod_hasher() {
        for i in 0..255u8 {
            assert_eq!(i, ModHasher::default().hash_one(i) as u8,)
        }
    }
}
