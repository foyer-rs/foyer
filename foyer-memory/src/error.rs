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

use std::fmt::Display;

/// In-memory cache error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Multiple error list.
    #[error(transparent)]
    Multiple(MultipleError),
    /// Config error.
    #[error("config error: {0}")]
    ConfigError(String),
}

impl Error {
    /// Combine multiple errors into one error.
    pub fn multiple(errs: Vec<Error>) -> Self {
        Self::Multiple(MultipleError(errs))
    }
}

#[derive(thiserror::Error, Debug)]
pub struct MultipleError(Vec<Error>);

impl Display for MultipleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multiple errors: [")?;
        if let Some((last, errs)) = self.0.as_slice().split_last() {
            for err in errs {
                write!(f, "{}, ", err)?;
            }
            write!(f, "{}", last)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

/// In-memory cache result.
pub type Result<T> = std::result::Result<T, Error>;
