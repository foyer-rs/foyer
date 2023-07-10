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

pub trait Weight {
    fn weight(&self) -> usize;
}

impl Weight for Vec<u8> {
    fn weight(&self) -> usize {
        self.len()
    }
}

pub use foyer_common as common;
pub use foyer_intrusive as intrusive;
pub use foyer_storage as storage;
