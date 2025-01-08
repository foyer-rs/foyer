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

use equivalent::Equivalent;
use foyer::{Cache, CacheBuilder};

#[derive(Hash, PartialEq, Eq)]
pub struct Pair<A, B>(pub A, pub B);

// FYI: https://docs.rs/equivalent
impl<'a, A: ?Sized, B: ?Sized, C, D> Equivalent<(C, D)> for Pair<&'a A, &'a B>
where
    A: Equivalent<C>,
    B: Equivalent<D>,
{
    fn equivalent(&self, key: &(C, D)) -> bool {
        self.0.equivalent(&key.0) && self.1.equivalent(&key.1)
    }
}

fn main() {
    let cache: Cache<(String, String), String> = CacheBuilder::new(16).build();

    let entry = cache.insert(
        ("hello".to_string(), "world".to_string()),
        "This is a string tuple pair.".to_string(),
    );
    // With `Equivalent`, `Pair(&str, &str)` can be used to compared `(String, String)`!
    let e = cache.get(&Pair("hello", "world")).unwrap();

    assert_eq!(entry.value(), e.value());
}
