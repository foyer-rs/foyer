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

use crate::{buffer::BufferError, device::DeviceError, serde::SerdeError};
use std::fmt::Debug;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("device error: {0}")]
    Device(#[from] DeviceError),
    #[error("buffer error: {0}")]
    Buffer(#[from] BufferError),
    #[error("serde error: {0}")]
    Serde(#[from] SerdeError),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = core::result::Result<T, Error>;
