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

/// Use `debug_assert!` by default. Use `assert!` when feature "strict_assertions" is enabled.
#[macro_export]
macro_rules! strict_assert {
    ($($arg:tt)*) => {
        #[cfg(feature = "strict_assertions")]
        assert!($($arg)*);
        #[cfg(not(feature = "strict_assertions"))]
        debug_assert!($($arg)*);
    }
}

/// Use `debug_assert_eq!` by default. Use `assert_eq!` when feature "strict_assertions" is enabled.
#[macro_export]
macro_rules! strict_assert_eq {
    ($($arg:tt)*) => {
        #[cfg(feature = "strict_assertions")]
        assert_eq!($($arg)*);
        #[cfg(not(feature = "strict_assertions"))]
        debug_assert_eq!($($arg)*);
    }
}

/// Use `debug_assert_ne!` by default. Use `assert_ne!` when feature "strict_assertions" is enabled.
#[macro_export]
macro_rules! strict_assert_ne {
    ($($arg:tt)*) => {
        #[cfg(feature = "strict_assertions")]
        assert_ne!($($arg)*);
        #[cfg(not(feature = "strict_assertions"))]
        debug_assert_ne!($($arg)*);
    }
}

/// Extend functions for [`Option`].
pub trait OptionExt<T>: Sized {
    /// Use `unwrap_unchecked` by default. Use `unwrap` when feature "strict_assertions" is enabled.
    ///
    /// # Safety
    ///
    /// See [`Option::unwrap_unchecked`].
    unsafe fn strict_unwrap_unchecked(self) -> T;
}

impl<T> OptionExt<T> for Option<T> {
    unsafe fn strict_unwrap_unchecked(self) -> T {
        #[cfg(feature = "strict_assertions")]
        {
            self.unwrap()
        }
        #[cfg(not(feature = "strict_assertions"))]
        {
            unsafe { self.unwrap_unchecked() }
        }
    }
}
