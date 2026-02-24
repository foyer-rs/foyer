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

use std::{fs::File, sync::Arc};

/// Media represents the underlying storage media for a device.
///
/// Media can be composed of different attributes and is suitable for different types of IO engines.
#[derive(Debug, Clone)]
pub struct Media {
    inner: Arc<MediaInner>,
}

#[derive(Debug)]
struct MediaInner {
    name: Option<String>,
    file: Option<File>,
}

impl Media {
    /// Create a new media.
    pub fn new(name: Option<String>, file: Option<File>) -> Self {
        let inner = MediaInner { name, file };
        Self { inner: Arc::new(inner) }
    }

    /// Get the name of the media.
    ///
    /// # Panics
    ///
    /// Panics if the media does not have a name.
    pub fn name(&self) -> &str {
        self.inner.name.as_ref().unwrap()
    }

    /// Get the file of the media.
    ///
    /// # Panics
    ///
    /// Panics if the media does not have a file.
    pub fn file(&self) -> &File {
        self.inner.file.as_ref().unwrap()
    }
}
