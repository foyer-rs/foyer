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

// /// Builder for [`File`].
// #[derive(Debug)]
// pub struct FileDeviceBuilder {
//     path: PathBuf,
//     capacity: Option<usize>,
//     throttle: Throttle,
//     #[cfg(target_os = "linux")]
//     direct: bool,
// }

// impl FileDeviceBuilder {
//     /// Use the given file path as the file device path.
//     pub fn new(path: impl AsRef<Path>) -> Self {
//         Self {
//             path: path.as_ref().into(),
//             capacity: None,
//             throttle: Throttle::default(),
//             #[cfg(target_os = "linux")]
//             direct: false,
//         }
//     }

//     /// Set the capacity of the file device.
//     ///
//     /// The given capacity may be modified on build for alignment.
//     ///
//     /// The file device uses 80% of the current free disk space by default.
//     pub fn with_capacity(mut self, capacity: usize) -> Self {
//         self.capacity = Some(capacity);
//         self
//     }

//     /// Set the throttle of the file device.
//     pub fn with_throttle(mut self, throttle: Throttle) -> Self {
//         self.throttle = throttle;
//         self
//     }

//     /// Set whether the file device should use direct I/O.
//     #[cfg(target_os = "linux")]
//     pub fn with_direct(mut self, direct: bool) -> Self {
//         self.direct = direct;
//         self
//     }
// }

#[derive(Debug)]
struct FileInner {
    file: StdFile,
    name: String,
    capacity: usize,
}

#[derive(Debug, Clone)]
pub struct File {
    inner: Arc<FileInner>,
    offset: u64,
    size: usize,
}

impl File {
    // pub fn open()
}
