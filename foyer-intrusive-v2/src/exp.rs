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

//! TODO(MrCroxx): Finish the docs.

use std::{marker::PhantomData, ptr::NonNull};

pub struct DefaultId;

pub struct Link {
    perv: Option<NonNull<Link>>,
    next: Option<NonNull<Link>>,
}

unsafe impl Send for Link {}
unsafe impl Sync for Link {}

pub trait Item<ID = DefaultId> {
    fn link(&mut self) -> &mut Link;
}

pub struct List<ID, T> {
    head: Option<NonNull<Link>>,
    tail: Option<NonNull<Link>>,

    len: usize,

    _marker: PhantomData<(ID, T)>,
}

unsafe impl<ID, T> Send for List<ID, T> {}
unsafe impl<ID, T> Sync for List<ID, T> {}

impl<ID, T> Default for List<ID, T>
where
    T: Item<ID>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ID, T> List<ID, T>
where
    T: Item<ID>,
{
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
            _marker: PhantomData,
        }
    }

    pub fn push_back(&mut self, item: &mut Box<T>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Something {
        l: Link,
        la: Link,
        lb: Link,
    }

    struct ListA;
    struct ListB;

    impl Item for Something {
        fn link(&mut self) -> &mut Link {
            &mut self.l
        }
    }

    impl Item<ListA> for Something {
        fn link(&mut self) -> &mut Link {
            &mut self.la
        }
    }

    impl Item<ListB> for Something {
        fn link(&mut self) -> &mut Link {
            &mut self.lb
        }
    }
}
