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

use std::sync::Arc;

use itertools::Itertools;

use crate::{
    eviction::{Eviction, Op},
    record::Record,
};
#[expect(dead_code)]
pub trait OpExt: Eviction {
    fn acquire_immutable(&self, record: &Arc<Record<Self>>) {
        match Self::acquire() {
            Op::Immutable(f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn acquire_mutable(&mut self, record: &Arc<Record<Self>>) {
        match Self::acquire() {
            Op::Mutable(mut f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn release_immutable(&self, record: &Arc<Record<Self>>) {
        match Self::release() {
            Op::Immutable(f) => f(self, record),
            _ => unreachable!(),
        }
    }

    fn release_mutable(&mut self, record: &Arc<Record<Self>>) {
        match Self::release() {
            Op::Mutable(mut f) => f(self, record),
            _ => unreachable!(),
        }
    }
}

impl<E> OpExt for E where E: Eviction {}

pub trait Dump: Eviction {
    type Output;
    fn dump(&self) -> Self::Output;
}

pub fn assert_ptr_eq<T>(a: &Arc<T>, b: &Arc<T>) {
    assert_eq!(Arc::as_ptr(a), Arc::as_ptr(b));
}

pub fn assert_ptr_vec_eq<T>(va: Vec<Arc<T>>, vb: Vec<Arc<T>>) {
    let trans = |v: Vec<Arc<T>>| v.iter().map(Arc::as_ptr).collect_vec();
    assert_eq!(trans(va), trans(vb));
}

pub fn assert_ptr_vec_vec_eq<T>(vva: Vec<Vec<Arc<T>>>, vvb: Vec<Vec<Arc<T>>>) {
    let trans = |vv: Vec<Vec<Arc<T>>>| vv.iter().map(|v| v.iter().map(Arc::as_ptr).collect_vec()).collect_vec();

    assert_eq!(trans(vva), trans(vvb));
}
