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

use std::fmt::Debug;

use foyer_common::code::Key;

use crate::core::pointer::PointerOps;

pub trait Link: Send + Sync + 'static + Default + Debug {
    fn is_linked(&self) -> bool;
}

/// # Safety
///
/// Pointer operations MUST be valid.
///
/// [`Adapter`] is recommanded to be generated by macro `instrusive_adapter!`.
pub unsafe trait Adapter: Send + Sync + 'static {
    type PointerOps: PointerOps;
    type Link: Link;

    fn new() -> Self;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn link2item(
        &self,
        link: *const Self::Link,
    ) -> *const <Self::PointerOps as PointerOps>::Item;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn item2link(
        &self,
        item: *const <Self::PointerOps as PointerOps>::Item,
    ) -> *const Self::Link;
}

/// # Safety
///
/// Pointer operations MUST be valid.
///
/// [`KeyAdapter`] is recommanded to be generated by macro `key_adapter!`.
pub unsafe trait KeyAdapter: Adapter {
    type Key: Key;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn item2key(
        &self,
        item: *const <Self::PointerOps as PointerOps>::Item,
    ) -> *const Self::Key;
}

/// # Safety
///
/// Pointer operations MUST be valid.
///
/// [`PriorityAdapter`] is recommanded to be generated by macro `priority_adapter!`.
pub unsafe trait PriorityAdapter: Adapter {
    type Priority: PartialEq + Eq + PartialOrd + Ord + Clone + Copy + Into<usize>;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn item2priority(
        &self,
        item: *const <Self::PointerOps as PointerOps>::Item,
    ) -> *const Self::Priority;
}

/// Macro to generate an implementation of [`Adapter`] for instrusive container and items.
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
/// use foyer_intrusive::{intrusive_adapter, key_adapter};
/// use foyer_intrusive::core::adapter::{Adapter, KeyAdapter, Link};
/// use foyer_intrusive::core::pointer::PointerOps;
/// use foyer_intrusive::eviction::EvictionPolicy;
/// use std::sync::Arc;
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
/// intrusive_adapter! { ItemAdapter<L> = Arc<Item<L>>: Item<L> { link: L} where L: Link }
/// key_adapter! { ItemAdapter<L> = Item<L> { key: u64 } where L: Link }
/// ```
#[macro_export]
macro_rules! intrusive_adapter {
    (@impl
        $vis:vis $name:ident ($($args:tt),*) = $pointer:ty: $item:path { $field:ident: $link:ty } $($where_:tt)*
    ) => {
        $vis struct $name<$($args),*> $($where_)* {
            _marker: std::marker::PhantomData<($pointer, $($args),*)>
        }

        unsafe impl<$($args),*> Send for $name<$($args),*> $($where_)* {}
        unsafe impl<$($args),*> Sync for $name<$($args),*> $($where_)* {}

        unsafe impl<$($args),*> $crate::core::adapter::Adapter for $name<$($args),*> $($where_)*{
            type PointerOps = $pointer;
            type Link = $link;

            fn new() -> Self {
                Self {
                    _marker: std::marker::PhantomData,
                }
            }

            unsafe fn link2item(
                &self,
                link: *const Self::Link,
            ) -> *const <Self::PointerOps as $crate::core::pointer::PointerOps>::Item {
                $crate::container_of!(link, $item, $field)
            }

            unsafe fn item2link(
                &self,
                item: *const <Self::PointerOps as $crate::core::pointer::PointerOps>::Item,
            ) -> *const Self::Link {
                (item as *const u8).add($crate::offset_of!($item, $field)) as *const _
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

/// Macro to generate an implementation of [`KeyAdapter`] for instrusive container and items.
/// ///
/// The basic syntax to create an adapter is:
///
/// ```rust,ignore
/// key_adapter! { Adapter = Item { key_field: KeyType } }
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
/// use foyer_intrusive::{intrusive_adapter, key_adapter};
/// use foyer_intrusive::core::adapter::{Adapter, KeyAdapter, Link};
/// use foyer_intrusive::core::pointer::PointerOps;
/// use foyer_intrusive::eviction::EvictionPolicy;
/// use std::sync::Arc;
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
/// intrusive_adapter! { ItemAdapter<L> = Arc<Item<L>>: Item<L> { link: L} where L: Link }
/// key_adapter! { ItemAdapter<L> = Item<L> { key: u64 } where L: Link }
/// ```
#[macro_export]
macro_rules! key_adapter {
    (@impl
        $adapter:ident ($($args:tt),*) = $item:ty { $field:ident: $key:ty } $($where_:tt)*
    ) => {
        unsafe impl<$($args),*> $crate::core::adapter::KeyAdapter for $adapter<$($args),*> $($where_)*{
            type Key = $key;

            unsafe fn item2key(
                &self,
                item: *const <Self::PointerOps as $crate::core::pointer::PointerOps>::Item,
            ) -> *const Self::Key {
                (item as *const u8).add($crate::offset_of!($item, $field)) as *const _
            }
        }
    };
    (
        $name:ident = $($rest:tt)*
    ) => {
        key_adapter! {@impl
            $name () = $($rest)*
        }
    };
    (
        $name:ident<$($args:tt),*> = $($rest:tt)*
    ) => {
        key_adapter! {@impl
            $name ($($args),*) = $($rest)*
        }
    };
}

/// Macro to generate an implementation of [`PriorityAdapter`] for instrusive container and items.
/// ///
/// The basic syntax to create an adapter is:
///
/// ```rust,ignore
/// priority_adapter! { Adapter = Item { priority_field: PriorityType } }
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
/// use foyer_intrusive::{intrusive_adapter, priority_adapter};
/// use foyer_intrusive::core::adapter::{Adapter, PriorityAdapter, Link};
/// use foyer_intrusive::core::pointer::PointerOps;
/// use foyer_intrusive::eviction::EvictionPolicy;
/// use std::sync::Arc;
///
/// #[derive(Debug)]
/// pub struct Item<L>
/// where
///     L: Link
/// {
///     link: L,
///     priority: usize,
/// }
///
/// intrusive_adapter! { ItemAdapter<L> = Arc<Item<L>>: Item<L> { link: L} where L: Link }
/// priority_adapter! { ItemAdapter<L> = Item<L> { priority: usize } where L: Link }
/// ```
#[macro_export]
macro_rules! priority_adapter {
    (@impl
        $adapter:ident ($($args:tt),*) = $item:ty { $field:ident: $priority:ty } $($where_:tt)*
    ) => {
        unsafe impl<$($args),*> $crate::core::adapter::PriorityAdapter for $adapter<$($args),*> $($where_)*{
            type Priority = $priority;

            unsafe fn item2priority(
                &self,
                item: *const <Self::PointerOps as $crate::core::pointer::PointerOps>::Item,
            ) -> *const Self::Priority {
                (item as *const u8).add($crate::offset_of!($item, $field)) as *const _
            }
        }
    };
    (
        $name:ident = $($rest:tt)*
    ) => {
        priority_adapter! {@impl
            $name () = $($rest)*
        }
    };
    (
        $name:ident<$($args:tt),*> = $($rest:tt)*
    ) => {
        priority_adapter! {@impl
            $name ($($args),*) = $($rest)*
        }
    };
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::collections::dlist::*;

    use crate::intrusive_adapter;

    use super::*;

    #[derive(Debug)]
    struct DListItem {
        link: DListLink,
        val: u64,
    }

    impl DListItem {
        fn new(val: u64) -> Self {
            Self {
                link: DListLink::default(),
                val,
            }
        }
    }

    intrusive_adapter! { DListItemAdapter = Box<DListItem>: DListItem { link: DListLink }}
    key_adapter! { DListItemAdapter = DListItem { val: u64 } }

    #[test]
    fn test_adapter_macro() {
        let mut l = DList::<DListItemAdapter>::new();
        l.push_front(Box::new(DListItem::new(1)));
        let v = l.iter().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1]);
    }
}
