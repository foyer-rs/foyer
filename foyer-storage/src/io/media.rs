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

use std::{fs::File as StdFile, sync::Arc};

/// Media represents the underlying storage IO unit for a device.
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
    /// Get the name of the IO unit.
    ///
    /// # Panics
    ///
    /// Panics if the IO unit does not have a name.
    pub fn name(&self) -> &str {
        self.inner.name.as_ref().unwrap()
    }

    /// Get the file of the IO unit.
    ///
    /// # Panics
    ///
    /// Panics if the IO unit does not have a file.
    pub fn file(&self) -> &File {
        &self.inner.file.as_ref().unwrap()
    }
}

#[derive(Debug, Default)]
pub struct MediaBuilder {
    name: Option<String>,
    file: Option<File>,
}

impl MediaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_file(mut self, file: File) -> Self {
        self.file = Some(file);
        self
    }

    pub fn build(self) -> Media {
        Media {
            inner: Arc::new(MediaInner {
                name: self.name,
                file: self.file,
            }),
        }
    }
}

#[derive(Debug)]
pub struct File {
    raw: Arc<StdFile>,
    offset: u64,
    size: usize,
}

impl File {
    pub fn raw(&self) -> &Arc<StdFile> {
        &self.raw
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

#[derive(Debug)]
pub struct FileBuilder {
    raw: Arc<StdFile>,
    offset: u64,
    size: usize,
}

impl FileBuilder {
    pub fn new(raw: impl Into<Arc<StdFile>>) -> Self {
        Self {
            raw: raw.into(),
            offset: 0,
            size: 0,
        }
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    pub fn build(self) -> File {
        File {
            raw: self.raw,
            offset: self.offset,
            size: self.size,
        }
    }
}
