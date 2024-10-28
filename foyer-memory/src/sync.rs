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

use std::ops::{Deref, DerefMut};

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub enum Lock<T> {
    RwLock(RwLock<T>),
    Mutex(Mutex<T>),
}

impl<T> Lock<T> {
    pub fn rwlock(val: T) -> Self {
        Self::RwLock(RwLock::new(val))
    }

    pub fn mutex(val: T) -> Self {
        Self::Mutex(Mutex::new(val))
    }

    pub fn read(&self) -> LockReadGuard<'_, T> {
        match self {
            Lock::RwLock(inner) => LockReadGuard::RwLock(inner.read()),
            Lock::Mutex(_) => unreachable!(),
        }
    }

    pub fn write(&self) -> LockWriteGuard<'_, T> {
        match self {
            Lock::RwLock(inner) => LockWriteGuard::RwLock(inner.write()),
            Lock::Mutex(inner) => LockWriteGuard::Mutex(inner.lock()),
        }
    }
}

pub enum LockReadGuard<'a, T> {
    RwLock(RwLockReadGuard<'a, T>),
    #[expect(dead_code)]
    Mutex,
}

impl<'a, T> Deref for LockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::RwLock(guard) => guard.deref(),
            Self::Mutex => unreachable!(),
        }
    }
}

pub enum LockWriteGuard<'a, T> {
    RwLock(RwLockWriteGuard<'a, T>),
    Mutex(MutexGuard<'a, T>),
}

impl<'a, T> Deref for LockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::RwLock(guard) => guard.deref(),
            Self::Mutex(guard) => guard.deref(),
        }
    }
}

impl<'a, T> DerefMut for LockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::RwLock(guard) => guard.deref_mut(),
            Self::Mutex(guard) => guard.deref_mut(),
        }
    }
}
