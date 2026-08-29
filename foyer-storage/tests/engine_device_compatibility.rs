// Copyright 2026 foyer Project Authors
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

//! Compile-time tests for engine-device compatibility declarations.

#[test]
fn engine_device_compatibility() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/engine_device_compatibility/supported.rs");
    tests.pass("tests/ui/engine_device_compatibility/downstream_extension.rs");
    tests.compile_fail("tests/ui/engine_device_compatibility/unsupported.rs");
}
