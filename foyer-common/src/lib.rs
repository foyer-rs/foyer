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

//! Shared components and utils for foyer.

#[cfg(feature = "tokio")]
extern crate tokio;
#[cfg(feature = "madsim")]
extern crate madsim_tokio as tokio;

/// Allow to enable debug assertions in release profile with feature "strict_assertion".
pub mod assert;
/// The util that convert the blocking call to async call.
pub mod asyncify;
/// The bitwise utils.
pub mod bits;
/// The [`bytes::Buf`] and [`bytes::BufMut`] extensions.
pub mod buf;
/// The trait for the key and value encoding and decoding.
pub mod code;
/// A concurrent count down util.
pub mod countdown;
/// Components for monitoring internal events.
pub mod event;
/// Future extensions.
pub mod future;
/// Provisioned hashers.
pub mod hasher;
/// The shared metrics for foyer.
pub mod metrics;
/// Extensions for [`std::option::Option`].
pub mod option;
/// The range extensions.
pub mod range;
/// A rate limiter that returns the wait duration for limitation.
pub mod rate;
///  A ticket-based rate limiter.
pub mod rated_ticket;
/// A runtime that automatically shutdown itself on drop.
pub mod runtime;
/// A kotlin like functional programming helper.
pub mod scope;
/// Tracing related components.
pub mod tracing;
