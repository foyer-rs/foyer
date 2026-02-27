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

#[cfg(unix)]
pub fn get_dev_capacity(path: impl AsRef<std::path::Path>) -> foyer_common::error::Result<usize> {
    use foyer_common::error::Error;

    const BLKGETSIZE64: u64 = 0x80081272;
    const DIOCGMEDIASIZE: u64 = 0x40086481;
    const DKIOCGETBLOCKSIZE: u64 = 0x40046418;
    const DKIOCGETBLOCKCOUNT: u64 = 0x40046419;

    use std::{fs::File, os::fd::AsRawFd};

    let file = File::open(path.as_ref())?;
    let fd = file.as_raw_fd();

    if cfg!(target_os = "linux") {
        let mut size: u64 = 0;
        let res = unsafe { libc::ioctl(fd, BLKGETSIZE64, &mut size) };
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(res).into());
        }
        Ok(size as usize)
    } else if cfg!(target_os = "freebsd") {
        let mut size: u32 = 0;
        let res = unsafe { libc::ioctl(fd, DIOCGMEDIASIZE, &mut size) };
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(res).into());
        }
        Ok(size as usize)
    } else if cfg!(target_os = "macos") {
        let mut block_size: u64 = 0;
        let mut block_count: u64 = 0;
        let res = unsafe { libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) };
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(res).into());
        }
        let res = unsafe { libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut block_count) };
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(res).into());
        }
        let size = block_size * block_count;
        Ok(size as usize)
    } else {
        use foyer_common::error::ErrorKind;

        Err(Error::new(
            ErrorKind::Unsupported,
            "get_dev_capacity() is not supported on this platform".to_string(),
        ))
    }
}
