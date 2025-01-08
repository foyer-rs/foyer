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

use std::{collections::HashSet, process::Command};

use anyhow::anyhow;

use crate::args::error::{Error, Result};

type IoEngine = String;

#[derive(Debug)]
pub struct Fio {
    io_engines: HashSet<IoEngine>,
}

impl Fio {
    pub fn init() -> Result<Self> {
        if !Self::available() {
            return Err(Error::FioNotAvailable);
        }

        let io_engines = Self::list_io_engines()?;

        Ok(Self { io_engines })
    }

    fn available() -> bool {
        let output = match Command::new("fio").arg("--version").output() {
            Ok(output) => output,
            Err(_) => return false,
        };
        output.status.success()
    }

    pub fn io_engines(&self) -> &HashSet<IoEngine> {
        &self.io_engines
    }

    fn list_io_engines() -> Result<HashSet<IoEngine>> {
        let output = Command::new("fio").arg("--enghelp").output()?;
        if !output.status.success() {
            return Err(anyhow!("fail to get available io engines with fio").into());
        }

        let io_engines = String::from_utf8_lossy(&output.stdout)
            .split('\n')
            .skip(1)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        Ok(io_engines)
    }
}
