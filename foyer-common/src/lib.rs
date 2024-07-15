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

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![warn(missing_docs)]

//! Shared components and utils for foyer.

/// Allow enable debug assertions in release profile with feature "strict_assertion".
pub mod assert;
/// The util that convert the blocking call to async call.
pub mod asyncify;
/// The bitwise utils.
pub mod bits;
/// The [`bytes::Buf`] and [`bytes::BufMut`] extensions.
pub mod buf;
/// The trait for the key and value encoding and decoding.
pub mod code;
/// Bloom filters.
pub mod compact_bloom_filter;
/// A concurrent count down util.
pub mod countdown;
/// Components for monitoring internal events.
pub mod event;
/// Future extensions.
pub mod future;
/// The shared metrics for foyer.
pub mod metrics;
/// A concurrent object pool.
pub mod object_pool;
/// The range extensions.
pub mod range;
/// A rate limiter that returns the wait duration for limitation.
pub mod rate;
///  A ticket-based rate limiter.
pub mod rated_ticket;
/// A runtime that automatically shutdown itself on drop.
pub mod runtime;
/// Tracing related components.
pub mod tracing;

/// File system utils.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub mod fs;
