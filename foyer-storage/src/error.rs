//  Copyright 2023 MrCroxx
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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("device error: {0}")]
    Device(#[from] super::device::error::Error),
    #[error("entry too large: {len} > {capacity}")]
    EntryTooLarge { len: usize, capacity: usize },
    #[error("checksum mismatch, checksum: {checksum}, expected: {expected}")]
    ChecksumMismatch { checksum: u64, expected: u64 },
    #[error("channel full")]
    ChannelFull,
    #[error("event listener error: {0}")]
    EventListener(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("fetch value error: {0}")]
    FetchValue(anyhow::Error),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

impl Error {
    pub fn device(e: super::device::error::Error) -> Self {
        Self::Device(e)
    }

    pub fn fetch_value(e: impl Into<anyhow::Error>) -> Self {
        Self::Other(e.into())
    }

    pub fn other(e: impl Into<anyhow::Error>) -> Self {
        Self::Other(e.into())
    }

    pub fn event_listener(
        e: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::EventListener(e.into())
    }
}

pub type Result<T> = core::result::Result<T, Error>;
