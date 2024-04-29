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

// TODO(MrCroxx): We need more tests!

use super::*;

#[test]
fn test_basic() {
    let mut l1 = SlabLinkedList::<Box<_>>::new();

    assert_eq!(l1.pop_front(), None);
    assert_eq!(l1.pop_back(), None);
    assert_eq!(l1.pop_front(), None);
    l1.push_front(Box::new(1));
    assert_eq!(l1.pop_front(), Some(Box::new(1)));
    l1.push_back(Box::new(2));
    l1.push_back(Box::new(3));
    assert_eq!(l1.len(), 2);
    assert_eq!(l1.pop_front(), Some(Box::new(2)));
    assert_eq!(l1.pop_front(), Some(Box::new(3)));
    assert_eq!(l1.len(), 0);
    assert_eq!(l1.pop_front(), None);
    l1.push_back(Box::new(1));
    l1.push_back(Box::new(3));
    l1.push_back(Box::new(5));
    l1.push_back(Box::new(7));
    assert_eq!(l1.pop_front(), Some(Box::new(1)));

    let mut l2 = SlabLinkedList::new();
    l2.push_front(2);
    l2.push_front(3);
    {
        assert_eq!(l2.front().unwrap(), &3);
        let x = l2.front_mut().unwrap();
        assert_eq!(*x, 3);
        *x = 0;
    }
    {
        assert_eq!(l2.back().unwrap(), &2);
        let y = l2.back_mut().unwrap();
        assert_eq!(*y, 2);
        *y = 1;
    }
    assert_eq!(l2.pop_front(), Some(0));
    assert_eq!(l2.pop_front(), Some(1));
}
