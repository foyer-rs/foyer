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

use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

/// # Safety
///
/// Pointer operations MUST be valid.
pub unsafe trait PointerOps {
    type Item: ?Sized;
    type Pointer;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn from_raw(&self, item: *const Self::Item) -> Self::Pointer;

    fn into_raw(&self, ptr: Self::Pointer) -> *const Self::Item;

    fn as_ptr(&self, ptr: &Self::Pointer) -> *const Self::Item;
}

/// # Safety
///
/// Pointer operations MUST be valid.
pub unsafe trait DowngradablePointerOps: PointerOps {
    type WeakPointer;

    fn downgrade(&self, ptr: &<Self as PointerOps>::Pointer) -> Self::WeakPointer;
}

#[derive(Debug)]
pub struct DefaultPointerOps<Pointer>(PhantomData<Pointer>);

impl<Pointer> DefaultPointerOps<Pointer> {
    /// Constructs an instance of `DefaultPointerOps`.
    #[inline]
    pub const fn new() -> DefaultPointerOps<Pointer> {
        DefaultPointerOps(PhantomData)
    }
}

impl<Pointer> Clone for DefaultPointerOps<Pointer> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Pointer> Copy for DefaultPointerOps<Pointer> {}

impl<Pointer> Default for DefaultPointerOps<Pointer> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<'a, T: ?Sized> PointerOps for DefaultPointerOps<&'a T> {
    type Item = T;
    type Pointer = &'a T;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> &'a T {
        &*raw
    }

    #[inline]
    fn into_raw(&self, ptr: &'a T) -> *const T {
        ptr
    }

    #[inline]
    fn as_ptr(&self, ptr: &&'a T) -> *const T {
        *ptr as *const _
    }
}

unsafe impl<'a, T: ?Sized> PointerOps for DefaultPointerOps<Pin<&'a T>> {
    type Item = T;
    type Pointer = Pin<&'a T>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Pin<&'a T> {
        Pin::new_unchecked(&*raw)
    }

    #[inline]
    fn into_raw(&self, ptr: Pin<&'a T>) -> *const T {
        unsafe { Pin::into_inner_unchecked(ptr) as *const T }
    }

    #[inline]
    fn as_ptr(&self, ptr: &Pin<&'a T>) -> *const T {
        ptr.get_ref() as *const _
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<NonNull<T>> {
    type Item = T;
    type Pointer = NonNull<T>;

    unsafe fn from_raw(&self, raw: *const T) -> NonNull<T> {
        NonNull::new_unchecked(raw as *mut _)
    }

    fn into_raw(&self, ptr: NonNull<T>) -> *const T {
        ptr.as_ptr()
    }

    #[inline]
    fn as_ptr(&self, ptr: &NonNull<T>) -> *const T {
        ptr.as_ptr()
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Box<T>> {
    type Item = T;
    type Pointer = Box<T>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Box<T> {
        Box::from_raw(raw as *mut T)
    }

    #[inline]
    fn into_raw(&self, ptr: Box<T>) -> *const T {
        Box::into_raw(ptr) as *const T
    }

    #[inline]
    fn as_ptr(&self, ptr: &Box<T>) -> *const T {
        ptr.as_ref() as *const _
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Pin<Box<T>>> {
    type Item = T;
    type Pointer = Pin<Box<T>>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Pin<Box<T>> {
        Pin::new_unchecked(Box::from_raw(raw as *mut T))
    }

    #[inline]
    fn into_raw(&self, ptr: Pin<Box<T>>) -> *const T {
        Box::into_raw(unsafe { Pin::into_inner_unchecked(ptr) }) as *const T
    }

    #[inline]
    fn as_ptr(&self, ptr: &Pin<Box<T>>) -> *const T {
        ptr.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Rc<T>> {
    type Item = T;
    type Pointer = Rc<T>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Rc<T> {
        Rc::from_raw(raw)
    }

    #[inline]
    fn into_raw(&self, ptr: Rc<T>) -> *const T {
        Rc::into_raw(ptr)
    }

    #[inline]
    fn as_ptr(&self, ptr: &Rc<T>) -> *const T {
        Rc::as_ptr(ptr)
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Pin<Rc<T>>> {
    type Item = T;
    type Pointer = Pin<Rc<T>>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Pin<Rc<T>> {
        Pin::new_unchecked(Rc::from_raw(raw))
    }

    #[inline]
    fn into_raw(&self, ptr: Pin<Rc<T>>) -> *const T {
        Rc::into_raw(unsafe { Pin::into_inner_unchecked(ptr) })
    }

    #[inline]
    fn as_ptr(&self, ptr: &Pin<Rc<T>>) -> *const T {
        ptr.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Arc<T>> {
    type Item = T;
    type Pointer = Arc<T>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Arc<T> {
        Arc::from_raw(raw)
    }

    #[inline]
    fn into_raw(&self, ptr: Arc<T>) -> *const T {
        Arc::into_raw(ptr)
    }

    #[inline]
    fn as_ptr(&self, ptr: &Arc<T>) -> *const T {
        Arc::as_ptr(ptr)
    }
}

unsafe impl<T: ?Sized> PointerOps for DefaultPointerOps<Pin<Arc<T>>> {
    type Item = T;
    type Pointer = Pin<Arc<T>>;

    #[inline]
    unsafe fn from_raw(&self, raw: *const T) -> Pin<Arc<T>> {
        Pin::new_unchecked(Arc::from_raw(raw))
    }

    #[inline]
    fn into_raw(&self, ptr: Pin<Arc<T>>) -> *const T {
        Arc::into_raw(unsafe { Pin::into_inner_unchecked(ptr) })
    }

    #[inline]
    fn as_ptr(&self, ptr: &Pin<Arc<T>>) -> *const T {
        ptr.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized> DowngradablePointerOps for DefaultPointerOps<Rc<T>> {
    type WeakPointer = std::rc::Weak<T>;

    fn downgrade(&self, ptr: &<Self as PointerOps>::Pointer) -> Self::WeakPointer {
        Rc::downgrade(ptr)
    }
}

unsafe impl<T: ?Sized> DowngradablePointerOps for DefaultPointerOps<Arc<T>> {
    type WeakPointer = std::sync::Weak<T>;

    fn downgrade(&self, ptr: &<Self as PointerOps>::Pointer) -> Self::WeakPointer {
        Arc::downgrade(ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::boxed::Box;
    use std::fmt::Debug;
    use std::mem;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;

    /// Clones a `PointerOps::Pointer` from a `*const PointerOps::Value`
    ///
    /// This method is only safe to call if the raw pointer is known to be
    /// managed by the provided `PointerOps` type.
    #[inline]
    unsafe fn clone_pointer_from_raw<T: PointerOps>(
        pointer_ops: &T,
        ptr: *const T::Item,
    ) -> T::Pointer
    where
        T::Pointer: Clone,
    {
        use std::mem::ManuallyDrop;
        use std::ops::Deref;

        /// Guard which converts an pointer back into its raw version
        /// when it gets dropped. This makes sure we also perform a full
        /// `from_raw` and `into_raw` round trip - even in the case of panics.
        struct PointerGuard<'a, T: PointerOps> {
            pointer: ManuallyDrop<T::Pointer>,
            pointer_ops: &'a T,
        }

        impl<'a, T: PointerOps> Drop for PointerGuard<'a, T> {
            #[inline]
            fn drop(&mut self) {
                // Prevent shared pointers from being released by converting them
                // back into the raw pointers
                // SAFETY: `pointer` is never dropped. `ManuallyDrop::take` is not stable until 1.42.0.
                let _ = self
                    .pointer_ops
                    .into_raw(unsafe { core::ptr::read(&*self.pointer) });
            }
        }

        let holder = PointerGuard {
            pointer: ManuallyDrop::new(pointer_ops.from_raw(ptr)),
            pointer_ops,
        };
        holder.pointer.deref().clone()
    }

    #[test]
    fn test_box() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Box<_>>::new();
            let p = Box::new(1);
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Box<i32> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_rc() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Rc<_>>::new();
            let p = Rc::new(1);
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Rc<i32> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_arc() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Arc<_>>::new();
            let p = Arc::new(1);
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Arc<i32> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_box_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Box<_>>::new();
            let p = Box::new(1) as Box<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Box<dyn Debug> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_rc_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Rc<_>>::new();
            let p = Rc::new(1) as Rc<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Rc<dyn Debug> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_arc_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Arc<_>>::new();
            let p = Arc::new(1) as Arc<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Arc<dyn Debug> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn clone_arc_from_raw() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Arc<_>>::new();
            let p = Arc::new(1);
            let raw = Arc::as_ptr(&p);
            let p2: Arc<i32> = clone_pointer_from_raw(&pointer_ops, raw);
            assert_eq!(2, Arc::strong_count(&p2));
        }
    }

    #[test]
    fn clone_rc_from_raw() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Rc<_>>::new();
            let p = Rc::new(1);
            let raw = Rc::as_ptr(&p);
            let p2: Rc<i32> = clone_pointer_from_raw(&pointer_ops, raw);
            assert_eq!(2, Rc::strong_count(&p2));
        }
    }

    #[test]
    fn test_pin_box() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Box<_>>>::new();
            let p = Pin::new(Box::new(1));
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Pin<Box<i32>> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_rc() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Rc<_>>>::new();
            let p = Pin::new(Rc::new(1));
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Pin<Rc<i32>> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_arc() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Arc<_>>>::new();
            let p = Pin::new(Arc::new(1));
            let a: *const i32 = &*p;
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            let p2: Pin<Arc<i32>> = pointer_ops.from_raw(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_box_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Box<_>>>::new();
            let p = Pin::new(Box::new(1)) as Pin<Box<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Box<dyn Debug>> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_pin_rc_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Rc<_>>>::new();
            let p = Pin::new(Rc::new(1)) as Pin<Rc<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Rc<dyn Debug>> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_pin_arc_unsized() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Arc<_>>>::new();
            let p = Pin::new(Arc::new(1)) as Pin<Arc<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = pointer_ops.into_raw(p);
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Arc<dyn Debug>> = pointer_ops.from_raw(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn clone_pin_arc_from_raw() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Arc<_>>>::new();
            let p = Pin::new(Arc::new(1));
            let raw = pointer_ops.into_raw(p);
            let p2: Pin<Arc<i32>> = clone_pointer_from_raw(&pointer_ops, raw);
            let _p = pointer_ops.from_raw(raw);
            assert_eq!(2, Arc::strong_count(&Pin::into_inner(p2)));
        }
    }

    #[test]
    fn clone_pin_rc_from_raw() {
        unsafe {
            let pointer_ops = DefaultPointerOps::<Pin<Rc<_>>>::new();
            let p = Pin::new(Rc::new(1));
            let raw = pointer_ops.into_raw(p);
            let p2: Pin<Rc<i32>> = clone_pointer_from_raw(&pointer_ops, raw);
            let _p = pointer_ops.from_raw(raw);
            assert_eq!(2, Rc::strong_count(&Pin::into_inner(p2)));
        }
    }
}
