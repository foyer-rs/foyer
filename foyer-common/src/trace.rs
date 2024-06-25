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

use minitrace::{future::InSpan, prelude::*};

use std::{
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};

use futures::Future;
use pin_project::pin_project;

/// Configurations for trace.
#[derive(Debug)]
pub struct TraceConfig {
    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation in us.
    pub record_hybrid_insert_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `get` operation in us.
    pub record_hybrid_get_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `obtain` operation in us.
    pub record_hybrid_obtain_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `remove` operation in us.
    pub record_hybrid_remove_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `fetch` operation in us.
    pub record_hybrid_fetch_threshold_us: AtomicUsize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            record_hybrid_insert_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_get_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_obtain_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_remove_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_fetch_threshold_us: AtomicUsize::from(1000 * 1000),
        }
    }
}

/// A wrapper for future to trace with minitrace.
#[pin_project]
pub struct Traced<F> {
    #[pin]
    future: InSpan<F>,
}

impl<F> Traced<F> {
    /// Create a traced future with the given label.
    pub fn new(future: F, label: &'static str) -> Self
    where
        F: Future,
    {
        Self {
            future: future.in_span(Span::enter_with_local_parent(label)),
        }
    }
}

impl<F> Future for Traced<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        this.future.as_mut().poll(cx)
    }
}
