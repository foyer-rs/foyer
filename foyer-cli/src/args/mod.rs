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

mod error;
mod fio;

use bytesize::ByteSize;
use clap::{ArgGroup, Args};
use fio::Fio;

#[derive(Debug, Args)]
#[command(group = ArgGroup::new("exclusive").required(true).args(&["file", "dir"]))]
pub struct ArgsArgs {
    /// File for disk cache data. Use `DirectFile` as device.
    ///
    /// Either `file` or `dir` must be set.
    #[arg(short, long)]
    file: Option<String>,

    /// Directory for disk cache data. Use `DirectFs` as device.
    ///
    /// Either `file` or `dir` must be set.
    #[arg(short, long)]
    dir: Option<String>,

    /// Size of the disk cache occupies.
    #[arg(short, long)]
    size: Option<ByteSize>,
}

pub fn run(args: ArgsArgs) {
    println!("{args:#?}");

    let fio = Fio::init().unwrap();

    println!("{:#?}", fio.io_engines());
}
