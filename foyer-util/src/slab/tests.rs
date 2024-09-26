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

// TODO(MrCroxx): We need more tests!

use super::*;

#[test]
fn test_token_null_pointer_optimization() {
    assert_eq!(std::mem::size_of::<Token>(), std::mem::size_of::<Option<Token>>());
}

#[test]
fn test_slab() {
    let mut slab = Slab::new();

    let t1 = slab.insert(1);
    let t2 = slab.insert(2);
    let t3 = slab.insert(3);

    assert_eq!(slab.get(t1).unwrap(), &1);
    assert_eq!(slab.get(t2).unwrap(), &2);
    assert_eq!(slab.get(t3).unwrap(), &3);
    assert_eq!(slab.len(), 3);

    *slab.get_mut(t2).unwrap() = 4;
    assert_eq!(slab.get(t2).unwrap(), &4);
    assert_eq!(slab.len(), 3);

    let v2 = slab.remove(t2).unwrap();
    assert_eq!(v2, 4);
    assert_eq!(slab.len(), 2);
}
