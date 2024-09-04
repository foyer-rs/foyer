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

use std::path::Path;

/// Returns the available space. Available space in unix is space reserved for priviliged user +
/// free space.
pub fn freespace(path: impl AsRef<Path>) -> std::io::Result<u64> {
    fs4::free_space(path)
}

#[cfg(test)]
mod tests {
    use std::{
        env::{current_dir, var},
        process::Command,
    };

    use itertools::Itertools;

    use super::*;

    #[test]
    #[ignore]
    fn test() {
        if var("CI").unwrap_or("".to_string()) != "true" {
            let dir = current_dir().unwrap();
            let path = dir.as_os_str().to_str().unwrap();

            println!("{}", path);

            let v1 = freespace(path).unwrap();
            let df = String::from_utf8(Command::new("df").args(["-P", path]).output().unwrap().stdout).unwrap();
            let bs: usize = df.trim().split('\n').next().unwrap().split_whitespace().collect_vec()[1]
                .strip_suffix("-blocks")
                .unwrap()
                .parse()
                .unwrap();
            let av: usize = df.trim().split('\n').last().unwrap().split_whitespace().collect_vec()[3]
                .parse()
                .unwrap();
            let v2 = bs * av;

            println!("{}", df);

            assert_eq!(v1 as usize, v2);
        }
    }
}
