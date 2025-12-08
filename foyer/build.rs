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

//! Build script.

use std::{fs, path::Path};

const README_PATH: &str = "../README.md";

fn main() {
    println!("cargo:rerun-if-changed={}", README_PATH);
    if let Ok(readme) = fs::read_to_string(README_PATH) {
        let mut out = String::new();
        let mut skip = false;

        for line in readme.lines() {
            if line.contains("<!-- rustdoc-ignore-start -->") {
                skip = true;
                continue;
            }
            if line.contains("<!-- rustdoc-ignore-end -->") {
                skip = false;
                continue;
            }
            if !skip {
                out.push_str(line);
                out.push('\n');
            }
        }

        let out_path = Path::new(&std::env::var("OUT_DIR").unwrap()).join("foyer-docs.md");
        fs::write(out_path.clone(), out).unwrap();
    }
}
