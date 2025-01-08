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

/// Extension for [`std::option::Option`].
pub trait OptionExt {
    /// Wrapped type by [`Option`].
    type Val;

    /// Consume the wrapped value with the given function if there is.
    fn then<F>(self, f: F)
    where
        F: FnOnce(Self::Val);
}

impl<T> OptionExt for Option<T> {
    type Val = T;

    fn then<F>(self, f: F)
    where
        F: FnOnce(Self::Val),
    {
        if let Some(val) = self {
            f(val)
        }
    }
}
