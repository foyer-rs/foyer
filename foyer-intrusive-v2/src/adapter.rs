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

//  Copyright 2020 Amari Robinson
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

//! Intrusive data structure adapter that locates between pointer and item.

use std::fmt::Debug;

/// Intrusive data structure link.
pub trait Link: Send + Sync + 'static + Default + Debug {
    /// Check if the link is linked by the intrusive data structure.
    fn is_linked(&self) -> bool;
}

/// Intrusive data structure adapter.
///
/// # Safety
///
/// Pointer operations MUST be valid.
///
/// [`Adapter`] is recommended to be generated by macro `intrusive_adapter!`.
pub unsafe trait Adapter: Send + Sync + Debug + 'static {
    /// Item type for the adapter.
    type Item: ?Sized;
    /// Link type for the adapter.
    type Link: Link;

    /// Create a new intrusive data structure link.
    fn new() -> Self;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn link2ptr(&self, link: std::ptr::NonNull<Self::Link>) -> std::ptr::NonNull<Self::Item>;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn ptr2link(&self, ptr: std::ptr::NonNull<Self::Item>) -> std::ptr::NonNull<Self::Link>;
}

/// Macro to generate an implementation of [`Adapter`] for intrusive container and items.
///
/// The basic syntax to create an adapter is:
///
/// ```rust,ignore
/// intrusive_adapter! { Adapter = Pointer: Item { link_field: LinkType } }
/// ```
///
/// # Generics
///
/// This macro supports generic arguments:
///
/// Note that due to macro parsing limitations, `T: Trait` bounds are not
/// supported in the generic argument list. You must list any trait bounds in
/// a separate `where` clause at the end of the macro.
///
/// # Examples
///
/// ```
/// use foyer_intrusive_v2::intrusive_adapter;
/// use foyer_intrusive_v2::adapter::Link;
///
/// #[derive(Debug)]
/// pub struct Item<L>
/// where
///     L: Link
/// {
///     link: L,
///     key: u64,
/// }
///
/// intrusive_adapter! { ItemAdapter<L> = Item<L> { link => L } where L: Link }
/// ```
#[macro_export]
macro_rules! intrusive_adapter {
    (@impl
        $vis:vis $name:ident ($($args:tt),*) = $item:ty { $($fields:expr)+ => $link:ty } $($where_:tt)*
    ) => {
        $vis struct $name<$($args),*> $($where_)* {
            _marker: std::marker::PhantomData<($($args),*)>
        }

        unsafe impl<$($args),*> Send for $name<$($args),*> $($where_)* {}
        unsafe impl<$($args),*> Sync for $name<$($args),*> $($where_)* {}

        unsafe impl<$($args),*> $crate::adapter::Adapter for $name<$($args),*> $($where_)*{
            type Item = $item;
            type Link = $link;

            fn new() -> Self {
                Self {
                    _marker: std::marker::PhantomData,
                }
            }

            unsafe fn link2ptr(&self, link: std::ptr::NonNull<Self::Link>) -> std::ptr::NonNull<Self::Item> {
                std::ptr::NonNull::new_unchecked($crate::container_of!(link.as_ptr(), $item, $($fields)+))
            }

            unsafe fn ptr2link(&self, item: std::ptr::NonNull<Self::Item>) -> std::ptr::NonNull<Self::Link> {
                std::ptr::NonNull::new_unchecked((item.as_ptr() as *mut u8).add(std::mem::offset_of!($item, $($fields)+)) as *mut Self::Link)
            }
        }

        impl<$($args),*> std::fmt::Debug for $name<$($args),*> $($where_)*{
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                f.debug_struct(stringify!($name)).finish()
            }
        }
    };
    (
        $vis:vis $name:ident = $($rest:tt)*
    ) => {
        intrusive_adapter! {@impl
            $vis $name () = $($rest)*
        }
    };
    (
        $vis:vis $name:ident<$($args:tt),*> = $($rest:tt)*
    ) => {
        intrusive_adapter! {@impl
            $vis $name ($($args),*) = $($rest)*
        }
    };
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use itertools::Itertools;

    use super::*;
    use crate::{dlist::*, intrusive_adapter};

    #[derive(Debug)]
    struct DlistItem {
        link: DlistLink,
        val: u64,
    }

    impl DlistItem {
        fn new(val: u64) -> Self {
            Self {
                link: DlistLink::default(),
                val,
            }
        }
    }

    intrusive_adapter! { DlistItemAdapter = DlistItem { link => DlistLink }}

    #[test]
    fn test_adapter_macro() {
        let mut l = Dlist::<DlistItemAdapter>::new();
        l.push_front(unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(DlistItem::new(1)))) });
        let v = l.iter().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1]);
        let _ = unsafe { Box::from_raw(l.pop_front().unwrap().as_ptr()) };
    }
}
