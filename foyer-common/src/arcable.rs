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

use std::sync::Arc;

pub enum Arcable<T> {
    Owned(T),
    Arc(Arc<T>),
}

impl<T> From<T> for Arcable<T> {
    fn from(value: T) -> Self {
        Self::Owned(value)
    }
}

impl<T> From<Arc<T>> for Arcable<T> {
    fn from(value: Arc<T>) -> Self {
        Self::Arc(value)
    }
}

impl<T> Arcable<T> {
    pub fn into_arc(self) -> Arc<T> {
        match self {
            Arcable::Owned(v) => Arc::new(v),
            Arcable::Arc(v) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arc<T>(v: impl Into<Arcable<T>>) -> Arc<T> {
        v.into().into_arc()
    }

    #[test]
    fn test_arcable() {
        let v = 42u64;
        let av = arc(v);
        let av: Arc<u64> = arc(av);
        let _aav: Arc<Arc<u64>> = arc(av);
    }
}
