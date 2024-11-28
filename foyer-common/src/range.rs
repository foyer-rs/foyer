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

use std::ops::{Add, Bound, Range, RangeBounds, Sub};

mod private {

    pub trait ZeroOne {
        fn zero() -> Self;
        fn one() -> Self;
    }

    macro_rules! impl_one {
        ($($t:ty),*) => {
            $(
                impl ZeroOne for $t {
                    fn zero() -> Self {
                        0 as $t
                    }

                    fn one() -> Self {
                        1 as $t
                    }
                }
            )*
        };
    }

    macro_rules! for_all_num_type {
        ($macro:ident) => {
            $macro! { u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64 }
        };
    }

    for_all_num_type! { impl_one }
}

use private::ZeroOne;

// TODO(MrCroxx): Use `trait_alias` after stable.
// pub trait Idx =
//     PartialOrd<Self> + Add<Output = Self> + Sub<Output = Self> + Clone + Copy + Send + Sync + 'static + ZeroOne;

/// The range extensions.
pub trait RangeBoundsExt<
    T: PartialOrd<T> + Add<Output = T> + Sub<Output = T> + Clone + Copy + Send + Sync + 'static + ZeroOne,
>: RangeBounds<T>
{
    /// Get the start bound of the range.
    fn start(&self) -> Option<T> {
        match self.start_bound() {
            Bound::Included(v) => Some(*v),
            Bound::Excluded(v) => Some(*v + ZeroOne::one()),
            Bound::Unbounded => None,
        }
    }

    /// Get the end bound of the range.
    fn end(&self) -> Option<T> {
        match self.end_bound() {
            Bound::Included(v) => Some(*v + ZeroOne::one()),
            Bound::Excluded(v) => Some(*v),
            Bound::Unbounded => None,
        }
    }

    /// Get the start bound with a default value of the range.
    fn start_with_bound(&self, bound: T) -> T {
        self.start().unwrap_or(bound)
    }

    /// Get the end bound with a default value of the range.
    fn end_with_bound(&self, bound: T) -> T {
        self.end().unwrap_or(bound)
    }

    /// Get the new range with the given range bounds.
    fn bounds(&self, range: Range<T>) -> Range<T> {
        let start = self.start_with_bound(range.start);
        let end = self.end_with_bound(range.end);
        start..end
    }

    /// Get the range size.
    fn size(&self) -> Option<T> {
        let start = self.start()?;
        let end = self.end()?;
        Some(end - start)
    }

    /// Check if the range is empty.
    fn is_empty(&self) -> bool {
        match self.size() {
            Some(len) => len == ZeroOne::zero(),
            None => false,
        }
    }

    /// Check is the range is a full range.
    fn is_full(&self) -> bool {
        self.start_bound() == Bound::Unbounded && self.end_bound() == Bound::Unbounded
    }

    /// Map the range with the given method.
    fn map<F, R>(&self, f: F) -> (Bound<R>, Bound<R>)
    where
        F: Fn(&T) -> R,
    {
        (self.start_bound().map(&f), self.end_bound().map(&f))
    }
}

impl<
        T: PartialOrd<T> + Add<Output = T> + Sub<Output = T> + Clone + Copy + Send + Sync + 'static + ZeroOne,
        RB: RangeBounds<T>,
    > RangeBoundsExt<T> for RB
{
}
