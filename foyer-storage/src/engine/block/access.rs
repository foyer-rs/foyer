// Copyright 2026 foyer Project Authors
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

use std::sync::Arc;

use crate::{
    IoHandle,
    io::{
        bytes::{IoBuf, IoBufMut},
        engine::IoHandleV2,
    },
};

pub trait BlockEngineAccess: Send + Sync + 'static {
    fn create_block(&self, size: usize) -> Arc<dyn BlockAccess>;
}

pub trait BlockAccess: Send + Sync + 'static {
    fn write_at(&self, offset: u64, data: Box<dyn IoBuf>) -> IoHandleV2<Box<dyn IoBuf>>;

    fn read_at(&self, offset: u64, data: Box<dyn IoBufMut>) -> IoHandleV2<Box<dyn IoBufMut>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_object_safe(_: &dyn BlockEngineAccess) {}
}
