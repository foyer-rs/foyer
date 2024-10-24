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

/// Scoped functional programming extensions.
pub trait Scope {
    /// Scoped with ownership.
    fn with<F, R>(self, f: F) -> R
    where
        Self: Sized,
        F: FnOnce(Self) -> R,
    {
        f(self)
    }

    /// Scoped with reference.
    fn with_ref<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Self) -> R,
    {
        f(self)
    }

    /// Scoped with mutable reference.
    fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        f(self)
    }
}
impl<T> Scope for T {}