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

use super::Eviction;
use crate::handle::Handle;

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub trait TestEviction: Eviction
where
    Self::Item: Handle,
{
    fn dump(&self) -> Vec<(<Self::Item as Handle>::Key, <Self::Item as Handle>::Value)>;
}
